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
  4. Polls the run using the list endpoint (`/executions?limit=N`) as primary,
     with the single-execution endpoint as fallback. Skyvia's single-execution
     endpoint has been observed to time out server-side under sustained load
     while the list endpoint stays responsive.
  5. Treats poll failures as transient. Bails only when no successful poll has
     completed for `max_minutes` minutes (silence threshold, capped at 60).
     Skyvia drives total run duration; the GitHub Actions `timeout-minutes` is
     the outer wall-clock safety cap.
  6. Parses the embedded `result` JSON for final row counts (live counters
     can lag)
  7. Treats Failed-with-mostly-success as soft-success
  8. Exits 0 on success / soft-success, 1 on hard failure
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


def request(method: str, path: str, body: dict | None = None, timeout: int = 120) -> dict:
    data = json.dumps(body).encode() if body else None
    headers = {'Authorization': API_TOKEN, 'Content-Type': 'application/json'}
    if method == 'POST' and not data:
        headers['Content-Length'] = '0'
    req = urllib.request.Request(f'{API_BASE}{path}', data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
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


# Skyvia error signatures that mean the run never opened its CounterPoint
# source connection (agent / SQL Server briefly unreachable). Seen overnight
# during NetOps patching AND mid-day as random blips (e.g. Jun 25 5:37 PM ET).
# Safe to re-trigger because no rows moved.
TRANSIENT_CONNECTION_SIGNATURES = (
    'provider creation failed',
    'cancellationtokensource',
    'a connection attempt failed',
    'transport-level error',
)


def is_transient_connection_failure(execution: dict) -> bool:
    """True only for a clean connection-open failure with NO rows processed.
    A failure that moved any rows (success>0 or errors>0) is a data problem and
    must NOT be retried. 'Silence' (uncertain state) is also not retried, since
    the run may still be alive and re-triggering could double-process."""
    if execution.get('state') not in ('Failed', 'Canceled'):
        return False
    if (execution.get('successRows') or 0) > 0 or (execution.get('errorRows') or 0) > 0:
        return False
    raw = execution.get('result')
    msg = ''
    if raw:
        try:
            msg = json.loads(raw).get('ErrorMessage') or ''
        except (json.JSONDecodeError, TypeError):
            msg = str(raw)
    msg = msg.lower()
    return any(sig in msg for sig in TRANSIENT_CONNECTION_SIGNATURES)


def trigger_or_resume(integration_id: int) -> int | None:
    log(f'Checking for active execution on integration {integration_id}...')
    try:
        active = request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions/active')
    except Exception as e:
        log(f'Active-check failed (will fall back to list endpoint): {e}')
        active = {}

    if active.get('runId'):
        log(f'Active execution found via /active: runId={active["runId"]} state={active.get("state")} — watching it')
        return active['runId']

    # Fallback resume check: scan recent runs for an active state. Protects against
    # /active returning empty during the same server-side slow period that breaks
    # the single-execution endpoint.
    try:
        listing = request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions?limit=5')
        for run in listing.get('data', []):
            if run.get('state') in ('Queued', 'Running', 'InProgress', 'Pending'):
                log(f'Active execution found via list: runId={run["runId"]} state={run["state"]} — watching it')
                return run['runId']
    except Exception as e:
        log(f'List-endpoint resume check failed: {e}')

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
    except Exception as e:
        log(f'TRIGGER FAILED: {e}')
        return None


def get_execution(integration_id: int, run_id: int) -> dict | None:
    """Get current state of a run.

    Skyvia's API splits runs across endpoints by lifecycle stage:
    - /executions/active returns the currently-running run (live counters)
    - /executions?limit=N returns only terminal runs (in-progress excluded)
    - /executions/{runId} works but times out server-side under load

    Poll order: /active for in-progress, list for terminal, direct as last resort.
    """
    # Is this run still active? Use /active for live state of in-progress runs.
    try:
        active = request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions/active')
        if active.get('runId') == run_id:
            return active
    except Exception as e:
        log(f'Active endpoint failed (will try list): {e}')

    # Not active (or /active failed) — check terminal runs.
    try:
        listing = request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions?limit=20')
        for run in listing.get('data', []):
            if run.get('runId') == run_id:
                return run
    except Exception as e:
        log(f'List endpoint failed: {e}')

    # Last resort. Direct endpoint frequently 500s under load on this workspace.
    try:
        return request('GET', f'/workspaces/{WORKSPACE}/integrations/{integration_id}/executions/{run_id}')
    except Exception as e:
        log(f'Direct endpoint also failed: {e}')
        return None


DARK_WINDOW_SECONDS = 180  # After this much silence following a Running state, infer completion


def poll_to_completion(integration_id: int, run_id: int, poll_seconds: int, max_minutes: int) -> tuple[bool, dict]:
    """Poll a run to terminal state.

    Skyvia's lifecycle has a "dark window": /active drops a run when state leaves
    Running, but the list endpoint can take hours to surface the terminal state.
    During that window, get_execution returns None even though the run finished.

    Strategy:
      - Track the last observed execution dict
      - If we previously saw Running with successRows > 0 and silence reaches
        DARK_WINDOW_SECONDS, infer Succeeded using the last observed counts
      - Otherwise, silence threshold from max_minutes is the bail trigger

    GitHub Actions timeout-minutes is the outer wall-clock safety cap.
    """
    silence_seconds = max_minutes * 60
    last_successful_poll = time.time()
    last_line = None
    last_seen_execution = None

    while True:
        execution = get_execution(integration_id, run_id)

        if not execution:
            silence = int(time.time() - last_successful_poll)

            # Dark-window completion: /active dropped a Running run before the
            # list endpoint caught up. Infer Succeeded using last observed counts.
            if (last_seen_execution
                    and last_seen_execution.get('state') == 'Running'
                    and last_seen_execution.get('successRows', 0) > 0
                    and silence >= DARK_WINDOW_SECONDS):
                inferred_success = last_seen_execution.get('successRows', 0)
                inferred_errors = last_seen_execution.get('errorRows', 0)
                log(f'Dark-window completion: silent={silence}s after last Running '
                    f'state success={inferred_success} errors={inferred_errors} — inferring Succeeded')
                synthetic = dict(last_seen_execution)
                synthetic['state'] = 'Succeeded'
                synthetic['_inferred'] = True
                return True, synthetic

            if silence > silence_seconds:
                log(f'GIVING UP: silence={silence}s exceeded threshold {silence_seconds}s')
                return False, {'state': 'Silence', 'silence_seconds': silence}
            log(f'Poll returned no result (silence={silence}s of {silence_seconds}s budget, retrying)')
            time.sleep(poll_seconds)
            continue

        last_successful_poll = time.time()
        last_seen_execution = execution
        state = execution.get('state', 'Unknown')
        live_success = execution.get('successRows', 0)
        live_errors = execution.get('errorRows', 0)
        line = f'{state} success={live_success} errors={live_errors}'
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

        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True, help='Step name from CHAIN_JSON')
    args = parser.parse_args()

    step = next((s for s in CHAIN if s['name'] == args.job), None)
    if not step:
        log(f'Unknown job {args.job!r}. Available: {[s["name"] for s in CHAIN]}')
        sys.exit(2)

    integration_id = step['integration_id']
    # Retry only transient connection failures (no rows moved). Defaults apply
    # if CHAIN_JSON doesn't override per-step. trigger_or_resume() resumes any
    # live run first, so a retry never double-triggers.
    max_attempts = step.get('retry_attempts', 3)
    backoff = step.get('retry_backoff_seconds', 120)
    log(f'=== Step {step["name"]} (integration {integration_id}) ===')

    success, execution = False, {}
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            log(f'--- attempt {attempt}/{max_attempts} ---')

        run_id = trigger_or_resume(integration_id)
        if run_id:
            success, execution = poll_to_completion(
                integration_id, run_id,
                step.get('poll_seconds', 60),
                step.get('max_minutes', 60),
            )
            if success:
                break
        else:
            log('Could not start or resume execution')
            execution = {'state': 'TriggerFailed'}

        # Retry a lost trigger or a clean transient connection failure only.
        retriable = (not run_id) or is_transient_connection_failure(execution)
        if attempt < max_attempts and retriable:
            why = 'trigger failed' if not run_id else 'transient connection failure (0 rows)'
            log(f'{why}; retrying in {backoff}s [{attempt}/{max_attempts} attempts used]')
            time.sleep(backoff)
            continue
        break

    log(f'Result: state={execution.get("state")} success={execution.get("successRows")} errors={execution.get("errorRows")}')
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
