# Proposed FCAPSule Integration

FCAPSule Lab is intentionally independent. Integration should happen over observable
boundaries rather than direct Python imports or shared application state.

## Current Local Workflow

The lab provides a narrow export bridge suitable for local demonstrations:

1. Start the five services with `make up`.
2. Enable lock contention with `make fault-on` and wait for a Prometheus rule to fire.
3. Run `make export-case`. It queries a two-minute window from Prometheus and Docker,
   writing the normalized case under `artifacts/`.
4. In FCAPSule, run `fcapsule ingest-case --case <path> --app-id checkout-lab`.
5. Open Operations and select **Build report** for the captured incident.

The bridge transfers a one-time bounded export. It does not make FCAPSule a Docker log
store, Prometheus replacement, or controller of this workload.

## Production Adapter Direction

Register `checkout-lab` in FCAPSule with these source endpoints:

| Domain | Lab source | Intended FCAPSule adapter behavior |
|---|---|---|
| PM | `http://localhost:9090` | query bounded Prometheus ranges and alert state |
| FM | Prometheus `/api/v1/alerts` | normalize active rules to FM events |
| Logs | `docker compose logs` or a log backend export | query/export only a bounded incident window |
| Topology | Compose service map | record orders -> inventory -> postgres relationships |
| Traces | not configured | record unavailable rather than invent trace context |

## Capture Direction

Use `make fault-on`, wait for the alert rules to fire, then have FCAPSule query a fixed
window around the first FM alert. The collector should retrieve only the PM range and
log window necessary for the incident. FCAPSule then runs its existing evidence pipeline
and produces the derived archive.

## Do Not Share Storage

Prometheus keeps metric history. Docker or a production log platform owns raw logs.
PostgreSQL owns application data. FCAPSule owns only application registration, incident
metadata, selected evidence, report artifacts, source references, and optional cited AI
briefings. The separation is intentional and should remain true when this moves to pods
or Kubernetes.

## Future Adapter Requirements

Before calling this a production integration, add a Prometheus range-query adapter,
Alertmanager or alert-state adapter, a structured-log adapter, source deep links, access
controls, and configurable source credentials. The lab is ready to exercise those
adapters without becoming part of FCAPSule's runtime.
