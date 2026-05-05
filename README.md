# Skyvia Integration Orchestrator

A scheduled GitHub Actions workflow that runs a chain of [Skyvia](https://skyvia.com) integrations in sequence, with built-in soft-failure tolerance and email-on-failure alerts.

## What it does

- Triggers a configurable chain of Skyvia integration runs in dependency order
- Watches each run via the Skyvia REST API; advances to the next step on success
- Tolerates "Failed" runs that have mostly-successful row counts (soft-success)
- Resumes a run mid-flight if a previous workflow attempt was killed
- Logs every state transition to GitHub Actions run history (90-day retention)
- Sends email alerts to repository watchers on hard failure

## Schedule

Runs Sunday and Wednesday at **02:00 UTC** (= 10:00 PM US/Eastern during EDT, 9:00 PM during EST).

Edit the `cron` line in `.github/workflows/sync.yml` to change.

## Required secrets

Set these under **Settings → Secrets and variables → Actions**:

| Secret | Description |
| --- | --- |
| `API_TOKEN` | Skyvia API key (workspace-scoped) |
| `WORKSPACE` | Skyvia workspace ID |
| `CHAIN_JSON` | JSON array describing the integration chain (see below) |

### `CHAIN_JSON` format

A JSON array of step descriptors. The `name` field must match the `--job` argument used in the workflow YAML (`step-1` through `step-7`).

```json
[
  {"name": "step-1", "integration_id": 0, "poll_seconds": 60, "max_minutes": 30},
  {"name": "step-2", "integration_id": 0, "poll_seconds": 60, "max_minutes": 30},
  {"name": "step-3", "integration_id": 0, "poll_seconds": 60, "max_minutes": 25},
  {"name": "step-4", "integration_id": 0, "poll_seconds": 120, "max_minutes": 200},
  {"name": "step-5", "integration_id": 0, "poll_seconds": 120, "max_minutes": 75},
  {"name": "step-6", "integration_id": 0, "poll_seconds": 120, "max_minutes": 350},
  {"name": "step-7", "integration_id": 0, "poll_seconds": 120, "max_minutes": 120}
]
```

Replace each `integration_id` with your Skyvia integration ID. `poll_seconds` is how often to check the run status; `max_minutes` is the hard timeout per step.

## Manual trigger

The workflow has `workflow_dispatch` enabled — run on demand from **Actions → sync → Run workflow**.

## Email alerts

GitHub sends an email to repository watchers when any workflow run fails, by default. Configure under your account: **Settings → Notifications → Workflow notifications → Email**.

To send to a specific address regardless of repo watch status, add an SMTP step using `dawidd6/action-send-mail` and set up SMTP credentials as additional secrets.

## Local development

```bash
export API_TOKEN=...
export WORKSPACE=...
export CHAIN_JSON='[...]'
python orchestrator.py --job step-1
```

## License

MIT
