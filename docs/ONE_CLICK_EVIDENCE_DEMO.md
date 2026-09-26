# One screenshot and a short voice note

## Browser workflow

1. In the Lab Demo track, start **Monitoring change and recovery**. The default
   three-minute lease restores the original monitoring configuration automatically.
   No terminal, local script or port-forward is required.
2. Open **Prometheus targets** from that scenario. The scrape pool
   `serviceMonitor/fcapsule-lab/fcapsule-lab-mysql/0` is preselected. Once DOWN, take **one**
   screenshot including the endpoint, HTTP error and target labels.
3. Let the Lab recover. Verify the target is UP again. In FCAPSule, open the new
   **LabExporterTargetUnavailable** incident and wait for its initial assessment.
4. Add the screenshot and record this short statement, only after verifying it:

   > This capture is from before the monitoring rollback. The target recovered
   > without restarting MySQL or the exporter, or changing labels.

5. In the audio's **Evidence details**, set the observation time to when you
   verified recovery. This distinguishes incident-time imagery from the later
   operator observation. Request the investigation update and compare revisions.

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

## Recorded live check: 2026-09-27

The scenario was started by clicking its Lab button in Chrome. Prometheus
observed HTTP 404 on the changed endpoint, the alert reached FCAPSule, and the
three-minute lease restored the original endpoint. The exporter remained the
same pod with zero restarts. Desktop (1440 px), mobile (390 px), keyboard access,
disabled controls during a run and the unavailable state were checked.

Exactly one external Prometheus screenshot and one 9.713-second synthetic voice
recording were attached. The recording was explicitly identified as synthetic;
its transcript reproduced the short statement above.

The first evidence revision was not an improvement: it mixed the dedicated
exporter monitor with an unrelated application's dropped target and treated the
undated audio cautiously. Adding the verified recovery observation time alone
was insufficient; that revision failed the next-action validation.

Investigation exposed a generic FCAPSule context-ranking defect: only DOWN
targets received exact-monitor priority. After recovery, an unrelated monitor
could appear first in the token-bounded context. The fix preserves the relevant
active target's priority for UP and unknown health as well. Regression tests cover
all three states; no scenario-specific diagnosis was added.

With the fixed context selection, the same two attachments produced a ready,
cited assessment:

| Before additional evidence | After additional evidence and context fix |
|---|---|
| Wrong path returns 404; check the exporter's served path and configuration. | Incident image shows the failing path; current discovery shows the restored path and healthy target. |
| Alternative: exporter may not serve metrics correctly. | No restart or label change was reported; a temporary scrape-path configuration change and rollback is better supported. |
| Next step is a broad endpoint/configuration check. | Review ServiceMonitor and Prometheus configuration history in the specific incident window. |

The final revision cited both attachments and the discovery check. It did not
claim the exact change author or precise historical configuration was proven.
The initial and final investigator calls used 9,485 and 8,790 tokens respectively;
intermediate diagnostic revisions are additional costs, not hidden successes.

This is an observed engineering validation, **not a controlled accuracy study**:
live state changed during recovery, a context bug was fixed, and audio observation
metadata was added. It demonstrates the completed workflow and useful temporal
reasoning, not a quantified causal gain attributable solely to multimodality.
