# One screenshot and a short voice note

## Browser workflow

1. In the Lab Demo track, start **Monitoring change and recovery**. The default
   three-minute lease restores the original monitoring configuration automatically.
   No terminal, local script or port-forward is required.
2. Open **Prometheus targets** from that scenario. Select scrape pool
   `serviceMonitor/fcapsule-lab/fcapsule-lab-mysql/0`. Once DOWN, take **one**
   screenshot including the endpoint, HTTP error and target labels.
3. Let the Lab recover. Verify the target is UP again. In FCAPSule, open the new
   **LabExporterTargetUnavailable** incident and wait for its initial assessment.
4. Add the screenshot and record this short statement, only after verifying it:

   > This capture is from before the monitoring rollback. The target recovered
   > without restarting MySQL or the exporter, or changing labels.

5. Request the investigation update and compare the saved revisions.

The Spanish equivalent is:

> La captura es anterior a la reversión del cambio de monitoreo. El target se
> recuperó sin reiniciar MySQL ni el exporter, y sin cambiar labels.

## What changes, and why the evidence matters

The controller temporarily changes only the existing exporter ServiceMonitor's
scrape path from `/metrics` to `/metrics-v2`. Prometheus really receives HTTP 404;
the database and exporter processes are not stopped. A normal `up == 0` rule
fires after 15 seconds. Source collection and alert delivery add latency.

The screenshot preserves the failing endpoint at the time of the incident.
The brief audio supplies operator chronology: rollback without a restart or
label change. This can reconcile failed historical scrapes with a healthy current
configuration, and distinguish a monitoring-path problem from a database outage.

This is an incremental evidence demonstration, not a guaranteed accuracy gain.
The automatic investigation may already identify the path mismatch, especially
when Collective remembers earlier occurrences. In that case the new contribution
is recovery chronology and reduced uncertainty, not a newly discovered cause.
Do not remove useful source data, alter the prompt to contain the answer, or
describe a longer response as a better diagnosis.

## Safety and repeatability

- The same single-run lock, node-memory guard and recovery watchdog apply.
- A durable ownership journal restores the original endpoint list after restart.
- Concurrent operator endpoint changes block automatic restoration rather than
  being overwritten. Resolve the conflict before starting another run.
- Kubernetes permission is limited to get/patch on this one Lab ServiceMonitor.
- The old guarded screenshot CLI remains available for reproducible evaluations.
- Previous cases can remain in local and Collective history. Use the explicit
  episode-and-shared-memory deletion only when a clean-history test is intended;
  deleting one occurrence does not erase unrelated occurrences or copied evidence.
