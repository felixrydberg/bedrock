# Bedrock agent instructions

## Find the relevant context

- For issue work, fetch the current issue and relevant dependencies with `gh issue view <number> --repo felixrydberg/bedrock`. Keep GitHub numbers and plan IDs together (for example, #26 / B-23a).
- Use the issue for scope and acceptance criteria; use `docs/implementation-prototype/feature-drafts.md` sections 1 and 4 for agreed architecture and section 2 for the dependency table. Later, uncreated work remains a draft.
- Explicit user decisions take precedence. Report contradictions before implementing affected behavior; continue independent work where the intended behavior is clear. Do not silently resolve conflicting requirements by inventing a design.
- Read only the documents needed: `kv.md` for storage/replication/read semantics, `cdn.md` for artifacts/retention, `code-structure.md` for module responsibilities, and `notes.md` for early product context. These files are under `docs/implementation-prototype/`.
- The checkout starts as scaffolding. Planned modules, binaries and test harnesses are not evidence that they already exist; inspect the actual files before using them.

## Architecture boundaries

- Put the shared distributed core in `crates/lib`. `kv`, `cdn`, `relay` and `api` are separate binaries, depend on `lib`, and never depend on each other.
- `bedrock-kv` hosts `quicksilver`, `kv` and `auth`; `bedrock-cdn` hosts `cdn` retention metadata and artifact bytes. Each subscribes only to its own datasets.
- The write API and relays are generic over datasets. Retention is ordinary SET/DELETE replication interpreted only by CDN apply hooks.
- `StorageEngine` and its adapters belong in `lib/storage`; KV keyspaces and eviction belong in `kv/storage`. Keep CDN artifact caching behind its separate `CacheStorage` contract.
- Membership comes from relay subscriber streams; peer reachability comes from real traffic. Do not introduce a separate heartbeat/gossip protocol or membership/placement epoch. Writer fencing is a separate concern.
- KV and CDN are backing services. Workers, Pages, activation and rollback belong to the future Compute/deploy control plane.
- Keep metrics labels low-cardinality. Expose namespace/key/hash/subscriber detail through introspection or the agreed top-N mechanism. Preserve the writer's `committed_at` through every hop.

## Correctness and validation

- Land #26 / B-23a before shared-core I/O work. Use runtime time/tasks, seeded randomness and simulation-compatible network/S3 seams. Keep direct filesystem, OS time and thread access inside adapters; avoid unordered map/set iteration when it affects messages or output.
- Simulation builds must exclude RocksDB and model buffered versus committed storage state. A seed must reproduce the same trace; keep failing seeds as regressions.
- Persist data and applied revisions atomically. Preserve ordered, duplicate-safe application, tombstones, checksums, minimum-revision reads and validation before readiness.
- A missing key is `Ok(None)`; corruption, unavailability and unsupported operations are explicit errors. Preserve isolation between datasets and services.
- Shared-core changes must meet the applicable #25 / B-23 invariants and checks. Protocol changes update the TLA+ specification first. Product tracks wait for the full hardening gate, including its nightly history.
- For Rust changes, run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`, plus the issue's relevant acceptance checks.
- Once #26 provides the harness, validate affected I/O paths in normal and `cfg(madsim)` builds and run the documented seed/replay checks. Never claim an absent harness or an unrun gate passed.
- Add a focused regression or contract test for non-trivial behavior. Documentation-only edits need link/content checks, not the Rust test suite.

## Route by ambiguity and consequence

Choose the cheapest available model that can reliably complete and verify the task. A short task can still require a strong model when a mistake changes durability, security or protocol semantics.

| Work | Claude Code | Codex |
| --- | --- | --- |
| Bounded discovery, dependency extraction, log summaries, mechanical edits with clear checks | Haiku 5.5 | GPT-6 Luna |
| Implementation against agreed interfaces, ordinary debugging, focused tests | Sonnet 5.5 | GPT-6.1 Sol |
| Ambiguous design, protocol changes, durability/security decisions, recovery and invariant review | Opus 5.5 | GPT-6.1 Sol with higher reasoning effort |

- The lead owns requirements, interfaces, protocol decisions and final integration. Cheap scouts return file references and evidence; the lead verifies findings that determine implementation scope or correctness.
- Start ordinary Sol/Sonnet work at medium effort, increasing it for unresolved reasoning. Keep lookup effort low; tune Luna's effort against its checks. Do not set maximum effort for every task.
- Escalate when requirements conflict, the agreed scope must expand, or a worker cannot explain a failed check. Return the evidence and unresolved question instead of repeatedly retrying the same approach.
- Select the actual worker model through the client's supported spawn options or custom agent configuration. This table is routing policy, not executable configuration; defaults may inherit the lead's model. Report unavailable models or substitutions.
- Prefer per-agent model selection over globally forcing every worker onto the cheapest model. These version choices were reviewed on 2026-10-24; revisit them when the available models change.

## Delegate independent work

- Use subagents when independent work has a clear benefit. Keep small edits and tightly sequential work with the lead. Start with at most two concurrent workers; workers do not delegate further unless requested.
- Give each worker the issue/plan ID, goal, relevant context, agreed interfaces, allowed files, acceptance checks and expected result. Send a focused brief instead of the entire conversation when possible.
- Agree shared contracts before parallel implementation. Only start work whose dependencies are complete; the dependency table and current issues take precedence over the plan's informal parallel-work summary.
- Assign disjoint files to simultaneous writers, or use isolated worktrees when appropriate. Separate context windows do not isolate files. One coordinator owns shared Cargo manifests, lockfile changes and integration.
- Workers return changed files or findings with locations, commands/check results, and unresolved risks. Return concise summaries instead of raw logs. Review workers report findings without silently editing the patch under review.
- After integration, the lead checks the combined changes and runs the relevant validation. Individual worker success does not prove the combined result works.

## Keep changes small

- Reuse existing contracts, helpers and dependencies. Search affected callers before changing a shared interface and update all affected callers and tests.
- Implement the issue's acceptance criteria without speculative features, dependencies or abstractions. Preserve validation, error handling and failure isolation.
- Preserve unrelated local work. Do not commit, publish or edit GitHub content when the user asked only for a draft.
- Report the outcome, checks actually run and material limitations concisely.
