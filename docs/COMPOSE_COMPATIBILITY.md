# Compose Compatibility Status

The PostgreSQL-based Compose stack remains in the repository for local development
and adapter compatibility checks. It is not one of the 17 supported Kubernetes
operator scenarios, is not included in the frozen 15-case diagnostic benchmark, and
has not been validated against the current live FCAPSule source and ownership
contracts.

In particular, the legacy `lock-contention` control has no equivalent Kubernetes
run lease or restart-safe owned recovery. The legacy `bad-database-config` mode uses
a separate invalid connection attempt rather than applying a bounded, observable
runtime configuration revision. Its static metrics also lack the Kubernetes
namespace/pod identity used by the current live source path. A passing local Compose
smoke test must therefore not be presented as proof that either case works in the
supported Kubernetes demo workflow.

Keep the Compose path as a compatibility aid, not as a second supported catalog. A
future decision to restore these cases requires a separate contract for owned
recovery, applied configuration provenance, safe secret handling, source identity,
and automated evaluation. Until then, use the Kubernetes scenarios documented in
[Operator demos](OPERATOR_DEMOS.md).
