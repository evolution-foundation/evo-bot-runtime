# Changelog

All notable changes to **evo-bot-runtime** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v1.0.0-rc6] - 2026-07-04

Restores agent conversation memory over a2a. The adapter keyed ADK sessions by numeric display ids instead of UUIDs, so the Processor's session key (`{contextId}_{agentID}`) never matched the persisted session — every message hit "404/500 Session not found" and started with zero history. Also makes dispatch media-aware and adds per-PR CI images for the review environment.

### Fixed

- **a2a session keying / agent memory**: send the conversation UUID as `contextId` (was the numeric conversation `display_id`), so the Processor's ADK session key matches the persisted session and conversation history is loaded instead of "404 Session not found" on every message.
- **a2a userId**: send the contact UUID as `userId` (was the numeric `ContactID`), aligning with the session pre-created by the CRM's SessionSync and fixing the ADK runner's "Session not found" → 500 even when the session and history existed. Both values are extracted from `metadata.evoai_crm_data`, with fallback to the numeric ids for legacy callers.

### Added

- **Media-aware dispatch**: media URLs are extracted from agent responses (Go mirror of the CRM's `MediaTypeDetector`) and delivered as structured attachments (`Attachments` field with `url`/`file_type`) in a dedicated postback, instead of arriving as plain links in text.

### Changed

- **CI**: pull requests now publish `:pr-N` and `:sha-<sha7>` images for the review environment; the `build-pr` job is gated to internal (non-fork) PRs. (EVO-1998)

### Deployment notes

- Deploy bot-runtime `v1.0.0-rc6` **after** the CRM `v1.0.0-rc6`, which processes the new `Attachments` postback field (version skew degrades gracefully — the CRM keeps its own media detection as fallback).
- No migrations, no new environment variables.

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

[Unreleased]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc6...HEAD
[v1.0.0-rc6]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc5...v1.0.0-rc6
[v1.0.0-rc5]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc3...v1.0.0-rc5
[v1.0.0-rc3]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc2...v1.0.0-rc3
[v1.0.0-rc2]: https://github.com/evolution-foundation/evo-bot-runtime/compare/v1.0.0-rc1...v1.0.0-rc2
[v1.0.0-rc1]: https://github.com/evolution-foundation/evo-bot-runtime/releases/tag/v1.0.0-rc1
