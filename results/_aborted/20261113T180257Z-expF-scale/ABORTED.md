# ABORTED — not evidence

Experiment F run 20261113T180257Z-expF-scale was stopped by hand during its
`clean_stop` step. The host Mac went into idle sleep on battery at about
13:08 New York (18:08 UTC) and woke at 13:26 (`pmset -g log`), which
suspended the Docker VM and the runner's monotonic clock mid-step. Its
measurements span a host sleep and must not be used. F was rerun under
`caffeinate`.
