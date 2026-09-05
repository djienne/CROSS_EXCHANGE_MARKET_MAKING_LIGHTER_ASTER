# XEMM agent notes

Follow [AGENTS.md](AGENTS.md), the [live runbook](LIVE_RUNBOOK.md), and the
[Docker setup](DOCKER_DEPLOY.md). Keep Rust checks in release mode and do not run
`cargo fmt`. The stack [README](../README.md) owns accounting and repair guidance.

`livebot --mode live` and live order probes submit real orders. Routine validation
uses unit/local-transport tests, offline replay and public-feed paper mode.
Required `aster.env` and `lighter.env` files remain local and ignored by git.
