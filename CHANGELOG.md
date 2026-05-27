# Changelog

All notable changes to **evo-bot-runtime** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v1.0.0-rc5] - 2026-05-27

Catch-up release. The `evo-bot-runtime` service skipped `v1.0.0-rc4` (no functional changes warranted a tag at that time); this `v1.0.0-rc5` tag realigns the bot-runtime image with the rest of the CRM Community family. No code or behavior changes — the Go binary is identical to `v1.0.0-rc3`.

## [v1.0.0-rc3] - 2026-05-06

Catch-up release published after the CRM Community family cycle to align the tag and Docker image with the rest of the family (`v1.0.0-rc3`). No functional changes in the service — the Go binary is identical to `v1.0.0-rc2`.

### Changed

- **Docs/branding**: README, CONTRIBUTING, LICENSE, NOTICE, SECURITY and TRADEMARKS standardized under Evolution Foundation; GitHub URLs migrated from `EvolutionAPI` to `evolution-foundation`.

## [v1.0.0-rc2] - 2026-05-05

Release with no functional changes in this service — only pipeline / staging adjustments.

### Changed

- **CI**: workflow now also publishes `develop` images to staging. (#1)

## [v1.0.0-rc1] - 2026-04-24

### Added

- First public release candidate of `evo-bot-runtime`.
- Go chatbot orchestration service (Bot Runtime).

---

[Unreleased]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc5...HEAD
[v1.0.0-rc5]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc3...v1.0.0-rc5
[v1.0.0-rc3]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc2...v1.0.0-rc3
[v1.0.0-rc2]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc1...v1.0.0-rc2
[v1.0.0-rc1]: https://github.com/evolution-foundation/evo-bot-runtime/releases/tag/v1.0.0-rc1
