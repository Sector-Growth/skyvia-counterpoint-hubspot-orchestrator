#!/usr/bin/env python3
"""
Skyvia integration orchestrator.

Runs a single named step from a chain of Skyvia integration runs.
All identifiers (workspace, integration IDs, chain order) come from environment.
Source contains no client-specific values; full chain config lives in CHAIN_JSON.

Behavior:
  1. Looks up the named step in CHAIN_JSON
  2. Checks for active execution; if found, watches it instead of triggering new
  3. Otherwise triggers a new execution
  4. Polls the run until terminal state, parsing the embedded `result` JSON for
     final row counts (the live counters can be stale)
  5. Treats Failed-with-mostly-success as soft-success
  6. Exits 0 on success / soft-success, 1 on hard failure
"""
import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

API_TOKEN = os.environ['API_TOKEN']
WORKSPACE = os.environ['WORKSPACE']
CHAIN = json.loads(os.environ['CHAIN_JSON'])
API_BASE = 'https://api.skyvia.com/v1'


def log(msg: str) -> None:
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def request(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body else None
    headers = {'Authorization': API_TOKEN, 'Content-Type': 'application/json'}
    if method == 'POST' and not data:
        headers['Content-Length'] = '0'
    req = urllib.request.Request(f'{API_BASE}{path}', data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        text = resp.read().decode()
        return json.loads(text) if text else {}


def parse_final_counts(execution: dict) -> tuple[int, int]:
    """Extract accurate row counts from the embedded result JSON.

    The live executions endpoint top-level success/error counters can stay at 0
    even when the run is finished. The truth lives in result.SuccessRows /
    result.ErrorRows once state is terminal.
    """
    success = execution.get('successRows', 0)
    errors = execution.get('errorRows', 0)
    raw_result = execution.get('result')
    if raw_result:
        try:
            parsed = json.loads(raw_result)
            success = parsed.get('SuccessRows', success)
            errors = parsed.get('ErrorRows', errors)
        except (json.JSONDecodeError, TypeError):
            pass
    return success, errors


def trigger_or_resume(integration_id: int) -> int | None:
    log(f'Checking for active execution on integration {integration_id}...')
    active = request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions/active')
    if active.get('runId'):
        log(f'Active execution found: runId={active["runId"]} state={active.get("state")} — watching it')
        return active['runId']

    log('No active execution — triggering new run')
    try:
        res = request('POST', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions')
        run_id = res.get('runId') or res.get('id')
        log(f'Triggered: runId={run_id}')
        return run_id
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        log(f'TRIGGER FAILED HTTP {e.code}: {body}')
        return None


def poll_to_completion(integration_id: int, run_id: int, poll_seconds: int, max_minutes: int) -> tuple[bool, dict]:
    started = time.time()
    cap = max_minutes * 60
    last_line = None

    while True:
        elapsed = int(time.time() - started)
        if elapsed > cap:
            log(f'HARD TIMEOUT after {elapsed}s')
            return False, {'state': 'Timeout', 'elapsed': elapsed}

        try:
            execution = request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions/{run_id}')
            state = execution.get('state', 'Unknown')
            live_success = execution.get('successRows', 0)
            live_errors = execution.get('errorRows', 0)
            line = f'{state} live success={live_success} errors={live_errors} elapsed={elapsed}s'
            if line != last_line:
                log(line)
                last_line = line

            if state in ('Succeeded', 'Failed', 'Canceled'):
                final_success, final_errors = parse_final_counts(execution)
                if (final_success, final_errors) != (live_success, live_errors):
                    log(f'Final counts (from result JSON): success={final_success} errors={final_errors}')
                execution['successRows'] = final_success
                execution['errorRows'] = final_errors

                if state == 'Succeeded':
                    return True, execution
                # Soft-success: terminal Failed but most rows succeeded.
                # Skyvia reports many runs this way (~0.05% errors out of tens of thousands).
                if state == 'Failed' and final_success > 0 and final_errors < max(50, final_success * 0.05):
                    log(f'Soft-success: {final_success} rows good vs {final_errors} errors')
                    return True, execution
                return False, execution
        except Exception as e:
            log(f'Poll error (will retry): {e}')

        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True, help='Step name from CHAIN_JSON')
    args = parser.parse_args()

    step = next((s for s in CHAIN if s['name'] == args.job), None)
    if not step:
        log(f'Unknown job {args.job!r}. Available: {[s["name"] for s in CHAIN]}')
        sys.exit(2)

    log(f'=== Step {step["name"]} (integration {step["integration_id"]}) ===')

    run_id = trigger_or_resume(step['integration_id'])
    if not run_id:
        log('Could not start or resume execution')
        sys.exit(1)

    success, execution = poll_to_completion(
        step['integration_id'],
        run_id,
        step.get('poll_seconds', 60),
        step.get('max_minutes', 60),
    )
    log(f'Result: state={execution.get("state")} success={execution.get("successRows")} errors={execution.get("errorRows")}')
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
