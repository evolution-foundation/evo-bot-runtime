<p align="center">
  <a href="https://evolutionfoundation.com.br">
    <img src="./public/hover-evolution.png" alt="Evolution Foundation" />
  </a>
</p>

<h1 align="center">Evo Bot Runtime</h1>

<p align="center">
  Bot pipeline execution, debouncing, and dispatch service for the Evo CRM Community ecosystem.
</p>

<p align="center">
  <a href="https://github.com/EvolutionAPI/evo-bot-runtime/releases/latest"><img src="https://img.shields.io/github/v/release/EvolutionAPI/evo-bot-runtime?include_prereleases&label=version&color=00ffa7" alt="Latest version" /></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License: Apache 2.0" /></a>
  <a href="https://docs.evolutionfoundation.com.br"><img src="https://img.shields.io/badge/Docs-evolutionfoundation.com.br-00ffa7" alt="Documentation" /></a>
  <a href="https://evolutionfoundation.com.br/community"><img src="https://img.shields.io/badge/Community-Join%20us-white" alt="Community" /></a>
</p>

<p align="center">
  <a href="https://evolutionfoundation.com.br">Website</a> &middot;
  <a href="https://docs.evolutionfoundation.com.br">Documentation</a> &middot;
  <a href="https://evolutionfoundation.com.br/community">Community</a> &middot;
  <a href="mailto:suporte@evofoundation.com.br">Support</a>
</p>

---

## About

**Evo Bot Runtime** is the bot orchestration service for the Evo CRM Community ecosystem. Built in Go with the Gin framework, it manages bot pipeline execution, message debouncing, and dispatch coordination across multiple WhatsApp instances and AI agents.

It integrates with `evo-ai-processor-community` (agent execution), `evo-ai-crm-community` (conversations) and Evolution API / Evolution Go (WhatsApp providers) to deliver real-time conversational flows with sub-second latency.

## Part of the Evo CRM Community

Evo Bot Runtime is part of the [Evo CRM Community](https://github.com/EvolutionAPI/evo-crm-community) ecosystem maintained by Evolution Foundation. To use the full stack, clone the umbrella repository with submodules:

```bash
git clone --recurse-submodules git@github.com:EvolutionAPI/evo-crm-community.git
```

The Community Edition is **single-tenant** by design — one account, no multi-tenancy overhead, no super-admin, no billing or plans. All limits are removed and features are unlocked by default.

---

## Quick Start

### Prerequisites

- **Go** 1.24+
- **PostgreSQL** 12+
- **Redis** 6+
- **Docker** (optional, recommended for production)

### Installation

```bash
git clone git@github.com:EvolutionAPI/evo-bot-runtime.git
cd evo-bot-runtime

# Install dependencies
go mod download

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Run
make run
```

The service will be available at `http://localhost:8080`.

### Docker

```bash
docker build -t evo-bot-runtime .
docker run -p 8080:8080 --env-file .env evo-bot-runtime
```

---

## Configuration

Copy `.env.example` to `.env` and configure the required variables:

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/evo_bot_runtime

# Redis (for debouncing and pipeline state)
REDIS_URL=redis://localhost:6379/0

# Service URLs
EVO_AI_PROCESSOR_URL=http://localhost:8000
EVO_AI_CRM_URL=http://localhost:3000

# Authentication (Bearer token from evo-auth-service-community)
JWT_SECRET_KEY=your_evolution_secret_key
```

See `.env.example` for the full list of variables.

---

## Architecture

Evo Bot Runtime sits between WhatsApp providers and AI agent execution:

```
WhatsApp message
      ↓
Evolution API / Evolution Go
      ↓
Evo Bot Runtime (debounce + pipeline)
      ↓
Evo AI Processor (agent execution)
      ↓
Evo CRM (conversation persistence)
```

Key responsibilities:
- **Debouncing**: groups rapid-fire messages from the same contact into a single agent invocation
- **Pipeline execution**: runs ordered bot flows with branching logic
- **Dispatch**: routes responses back to the originating WhatsApp instance
- **State**: persists pipeline progress in Redis with PostgreSQL fallback

---

## Documentation

| Resource | Link |
|---|---|
| Website | [evolutionfoundation.com.br](https://evolutionfoundation.com.br) |
| Documentation | [docs.evolutionfoundation.com.br](https://docs.evolutionfoundation.com.br) |
| Community | [evolutionfoundation.com.br/community](https://evolutionfoundation.com.br/community) |
| Changelog | [CHANGELOG.md](./CHANGELOG.md) |
| Contributing | [CONTRIBUTING.md](./CONTRIBUTING.md) |
| Security | [SECURITY.md](./SECURITY.md) |

---

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines on how to submit issues, propose features, and open pull requests.

Join our [community](https://evolutionfoundation.com.br/community) to discuss ideas and collaborate.

---

## Security

For security issues, **do not open a public issue**. Email **suporte@evofoundation.com.br** or use GitHub's private vulnerability reporting. See [SECURITY.md](./SECURITY.md) for details.

---

## License

Evo Bot Runtime is licensed under the Apache License 2.0. See [LICENSE](./LICENSE) for details.

## Trademarks

"Evolution Foundation", "Evolution" and "Evo Bot Runtime" are trademarks of Evolution Foundation. See [TRADEMARKS.md](./TRADEMARKS.md) for the brand assets policy.

Third-party attributions are documented in [NOTICE](./NOTICE).

---

<p align="center">
  Made by <a href="https://evolutionfoundation.com.br">Evolution Foundation</a> · © 2026
</p>
