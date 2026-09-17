# Product Review

## User Job

The lab exists to let a developer or evaluator generate a believable, observable failure
outside FCAPSule. The user should be able to start it with one Compose command, inspect
live logs from a terminal, introduce a failure with one command, see metrics and alerts
in Prometheus, and later compare that raw source behavior with FCAPSule's retained
report.

## Decisions

* Five containers are enough to model a user-facing API, a dependency API, a real
  database, a load source, and an independent metric collector without overwhelming a
  typical development machine.
* The default load targets at least 10,000 logs/minute across the application services,
  but avoids an uncontrolled infinite log-noise loop.
* Lock contention is the default incident because it creates causal ambiguity that needs
  FM, PM, logs, and topology to investigate. It is more representative than a hard-coded
  endpoint exception and safer than a host-dependent OOM test.
* Prometheus remains independent. FCAPSule consumes evidence from it later; it is not an
  embedded FCAPSule component.
* The lab exposes no product UI. Terminal control and the Prometheus UI are enough for
  its narrow role. The user-facing investigation UI remains FCAPSule Operations.

## Acceptance Criteria

The stack is ready for a Docker-host validation when it can start five healthy
containers, exceed the log-volume target, show three healthy Prometheus targets, trigger
the supplied alert rules after `make fault-on`, recover after `make fault-off`, and pass
`make verify`.
