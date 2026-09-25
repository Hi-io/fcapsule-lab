# Incident Library

The Lab has two distinct collections. **Demo track** contains previously qualified
investigations. **Incident library** adds 100 exploratory cases across 20 application
areas. Library cases are not scored examples or claims about FCAPSule's diagnostic
accuracy. They are meant to broaden the conditions available for later evaluation.

Each library entry describes an operator-visible symptom, a plausible precursor,
affected service and impact. The entry selects one of 22 bounded failure mechanisms.
One lightweight workload executes the selected mechanism after an operator starts a
leased run. It performs real local operations such as SQLite writes, an HTTP request,
DNS resolution, socket connection, queue insertion, credential validation, or bounded
resource work. Five contextual profiles per area share this executor; the Lab does not
deploy 100 independent services. The `simulated_service` and `scenario_context` log
fields identify the role being simulated while the actual Kubernetes pod and namespace
remain truthful.

The Lab controller applies its existing single-run ownership, node-memory admission,
expiry and recovery checks. The separate `lab-incident-library` workload emits JSON
logs and Prometheus metrics. A shared alert fires after repeated failures while a case
is active; it carries `scenario_id` but does not embed an evaluator answer. Runs are
limited to five minutes. The new workload requests 48 MiB and is limited to 192 MiB.
The memory mechanism allocates only 2 MiB and checks an application budget, so it
does not intentionally OOM the cluster. No external endpoint is contacted by the
failure executor: HTTP calls target its own loopback test server, and failed TCP
connections target loopback. The DNS mode uses an invalid local hostname.

Use the Lab UI's **Incident library** section to search by name, service or category.
The entry's `summary` is intentionally symptom-led. `precursor`, `impact` and
mechanism parameters live only in the Lab repository as development context. They
are not sent to FCAPSule as ground truth. Cases can also be started through the
existing `/api/scenarios/{id}/start` controller route and recovered with
`/api/recover`. Only one Lab intervention can run at a time.

The shared alert is `LabLibraryOperationFailures`. Prometheus discovers the new
workload through the existing application `ServiceMonitor`; the error log and metric
both carry the case ID for correlation. A case's labels and story are fictional and
must not be interpreted as a real incident in the named organization or region.

## Adding Cases

Each `app/library_cases/batch_NN.json` file contains five objects with these fields:
`id`, `title`, `category`, `service`, `mechanism`, `summary`, `precursor`, `impact`,
`parameters` and `context`. IDs are stable, batch-prefixed, and unique. The loader
validates every batch at startup. `parameters` record the concrete scenario profile;
the executor currently exercises a fixed bounded operation for each mechanism. It
does not model every numeric profile value as a physical measurement. Future work
can replace individual mechanisms with richer services without changing the catalog
identity or the demo track.
