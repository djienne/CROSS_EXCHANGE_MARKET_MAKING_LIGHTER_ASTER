# Repository Instructions

- Always use release-mode cargo verification in this repository, e.g. `cargo test --release` and `cargo build --release`; avoid debug cargo builds/tests so `target/debug` artifacts do not accumulate.
- `cargo fmt` is forbidden in this repository. Keep edits narrowly formatted by hand to avoid broad formatting churn.
- After implementing live-bot behavior/config changes from a plan, git commit the validated changes and restart the live bot with the release binary unless the user explicitly says not to.
