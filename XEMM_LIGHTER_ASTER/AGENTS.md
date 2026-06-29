# Repository Instructions

- Always build and verify runnable binaries with `cargo build --release`; do not rely on debug binaries for live-bot checks.
- `cargo fmt` is forbidden in this repository. Keep edits narrowly formatted by hand to avoid broad formatting churn.
- After implementing live-bot behavior/config changes from a plan, git commit the validated changes and restart the live bot with the release binary unless the user explicitly says not to.
