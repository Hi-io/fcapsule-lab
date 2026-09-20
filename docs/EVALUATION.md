# Paired Model Evaluation

## Purpose

The evaluator measures whether FCAPSule turns a retained incident capsule into a
causally useful investigation. It does not test whether a model can guess a scenario
name and it does not place benchmark state in the FCAPSule product database.

Each case produces real application traffic, logs, Prometheus metrics, Kubernetes
configuration and a symptom alert. FCAPSule captures the episode normally. The Lab
then reuses FCAPSule's existing settings and investigation endpoints to run multiple
models over the same retained evidence. No evaluation-specific endpoint is needed in
the main project.

## Fair Comparison

The runner counterbalances execution order. For the first case the first configured
model handles the automatic investigation; the second model is an explicit retry. The
order rotates on the next case. Both saved outputs must contain the same
`input_fingerprint`, otherwise `comparison_valid` is false and no score difference
should be interpreted as a model effect.

The runner stores for every model:

- the complete unedited investigation JSON;
- causal, evidence, action and epistemic-safety component scores;
- cited and required evidence domains;
- contradictions and matched findings;
- elapsed investigation time and provider token usage;
- the FCAPSule episode ID and evidence fingerprint.

The original active model and token limit are restored in a `finally` block. Lab
recovery is also attempted after failures. Raw outputs live in ignored
`artifacts/evaluation-<UTC>/`; only benchmark design and code belong in Git.

## Commands

Run one integration case before a full benchmark:

```bash
python3 tools/evaluate_models.py --scenario response-contract \
  --models deepseek-v4-flash deepseek-v4-pro
```

Run the fifteen-case paired suite:

```bash
python3 tools/evaluate_models.py --scenario all \
  --models deepseek-v4-flash deepseek-v4-pro
```

Estimate variance with repeated, counterbalanced runs:

```bash
python3 tools/evaluate_models.py --scenario all --repetitions 3 \
  --models deepseek-v4-flash deepseek-v4-pro
```

Use `--lab`, `--prometheus` and `--fcapsule` for another cluster. `--max-tokens`
uses the same completion ceiling for every candidate. `--minimum-log-lines` changes
the pipeline acceptance threshold, not the diagnostic score.

## Outputs

`REPORT.md` is the readable paired matrix. `results.csv` supports plotting and
statistical analysis. Each case directory contains `run.json`, bounded raw workload
logs, the independent alert record, one investigation JSON per model and one score
JSON per model. Ground truth is referenced by scenario ID but never copied into the
model response files.

The aggregate reports mean and median diagnostic score, tokens and case count. A
serious report should also show per-category results, paired wins/ties, invalid
comparisons and repeated-run dispersion. A higher score on this suite supports a
model-selection decision for FCAPSule; it does not prove superiority outside these
operational tasks.

