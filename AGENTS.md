# Repository Instructions

- Do not run `cargo fmt` anywhere in this stack unless the user explicitly overrides this instruction. Keep edits narrowly formatted by hand to avoid broad formatting churn.
- Always use release-mode cargo (`cargo build --release`, `cargo test --release`) unless the user says otherwise, so `target/debug` artifacts do not accumulate.
- Delete any consumed patch or diff files after applying them, so stale artifacts do not confuse later work.
- After implementing live-bot behavior/config changes from a plan, commit the validated changes and restart the live bot (`run --mode live`, see `LIGHTER_ASTER_BOT/RUNBOOK.md`) with the release binary unless the user explicitly says not to.
- `run --mode live`, `taker run` without `--observe-only`, and the `*-market`/`*-roundtrip` probes submit real orders. Routine validation uses unit/local-transport tests.
- `aster.env` and `lighter.env` stay local and ignored by git.
