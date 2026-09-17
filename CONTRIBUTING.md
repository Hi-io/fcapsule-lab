# Contributing

Keep the lab independent from FCAPSule: do not import FCAPSule modules or write directly
to its state directory. New scenarios should be reproducible, have bounded resource use,
emit structured logs, expose stable bounded-cardinality metrics, and document the
expected FM, PM, log, topology, and trace behavior.

Run the static checks before opening a change:

```bash
python3 -m compileall -q app tools
python3 tools/verify_stack.py  # on a Docker-enabled host with the stack running
```

Do not add credentials, generated Docker volumes, raw log captures, or API keys to Git.
