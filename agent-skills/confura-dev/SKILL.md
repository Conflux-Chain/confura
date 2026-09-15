---
name: confura-dev
description: Develop, review, deploy, configure, operate, and troubleshoot Confura in the Conflux-Chain/confura repository. Use when an AI agent needs to inspect or change Confura code, understand architecture, run Go tests, configure Core Space or eSpace services, deploy with Docker, tune sync/node/rpc/virtual-filter components, manage rate limits, ACLs, node routes, Grafana dashboards, or debug self-hosted Confura. Do not use for end-user questions that only ask how to call public Conflux RPC endpoints; use a public RPC user skill or public user documentation instead.
---

# Confura Dev

## Overview

Use this skill for Confura repository work aimed at maintainers, contributors, and self-hosted operators. Treat Confura as a Go public RPC gateway with separate Core Space and eSpace paths, plus sync, node management, virtual filter, storage, and rate-control subsystems.

Keep public endpoint usage questions out of this skill. If the user only asks how to call the hosted public RPC, which endpoint to use, or what to do when a hosted public RPC rate limit is hit, use a public RPC user skill when the agent environment provides one, or answer from public user docs without loading Confura internals.

## Portability

This is a model-agnostic Agent Skill. It intentionally avoids platform-specific metadata, tool names, or client assumptions. Any agent runtime can use it by reading this `SKILL.md` and then loading the referenced files on demand.

If an agent client supports platform adapters, keep those adapters outside this directory. Examples include UI metadata, client-specific settings, IDE prompt snippets, or MCP server manifests. This directory should remain the portable source of truth.

## First Decision

Classify the task before loading references:

- Code development or review: read `references/repo-map.md`, then `references/development-workflow.md`, and only then inspect relevant source files.
- Architecture explanation: read `references/architecture-map.md`; add `references/rpc-and-routing.md` for RPC, routing, logs, bridge, or filter behavior.
- Deployment or configuration: read `references/deployment-and-config.md`; add `references/cli-cheatsheet.md` for process commands.
- Operations or troubleshooting: read `references/operations-runbooks.md`; add the deployment, RPC, or CLI reference that matches the failing component.
- Rate limit, ACL, or node route administration: read `references/cli-cheatsheet.md` and `references/deployment-and-config.md`; inspect `cmd/ratelimit`, `cmd/acl`, `cmd/noderoute`, and `store/mysql` when changing behavior.
- Any non-trivial code change: read `references/system-invariants.md` before editing, then use `references/implementation-playbooks.md` to avoid missing companion changes.

## Working Rules

- Always determine Core Space vs eSpace before giving commands, config keys, or source paths. Many components have paired `cfx` and `eth` variants.
- Start repository work by checking current files and dirty state. Preserve unrelated user changes.
- Prefer `rg` and targeted reads to broad scans. Use existing docs as source anchors: `README.md`, `doc/DEPLOY.md`, `doc/ARCHITECTURE.md`, `doc/RPC_FEATURES.md`, `doc/INTERNAL_CONTRACT_EVENT_LOGS.md`, and `grafana/README.md`.
- For code changes, follow local Go style, run `gofmt` on touched `.go` files, and run targeted `go test` packages. Use `go test ./...` when shared behavior, config loading, RPC middleware, storage, or routing contracts change.
- For deployment guidance, make users choose or confirm network, space, persistence, fullnode endpoints, exposed modules, and whether the target is local Docker, staging, or production.
- Do not expose secrets. Redact DSNs, API keys, billing keys, alert webhooks, and private node URLs in summaries and examples.
- Avoid running destructive operational commands against live services unless the user explicitly asks and the target is clear.

## Reference Index

- `references/repo-map.md`: package ownership and where to inspect for common changes.
- `references/system-invariants.md`: source-level contracts and hidden assumptions agents must preserve.
- `references/implementation-playbooks.md`: concrete edit recipes for common Confura changes.
- `references/development-workflow.md`: build, test, style, and PR workflow.
- `references/architecture-map.md`: component responsibilities and request/data flow.
- `references/rpc-and-routing.md`: RPC server behavior, enhanced logs, routing, bridge, diagnostics.
- `references/deployment-and-config.md`: config sources, Docker Compose, ports, services, metrics.
- `references/operations-runbooks.md`: concise troubleshooting flows for common operator issues.
- `references/cli-cheatsheet.md`: Confura subcommands and admin utilities.
