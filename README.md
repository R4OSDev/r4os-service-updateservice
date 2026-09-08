# UPDSVC.R4X

`UPDSVC.R4X` is an independent R4OS service implemented in Zig.

## Package

- Version: `0.2.20`
- Image target: `/R4OS/SERVICES/UPDSVC.R4X`
- Image scope: `slim`
- Canonical project manifest: `module.R4MF`

The manifest is the single source of truth for the artifact, imports, image
target, and package metadata.

## Build

On Windows:

    Build.bat

On Linux or macOS:

    ./Build.sh

The build starters resolve the current local R4OS dependency checkouts through
`Settings.R4S`. The URL and hash entries in `build.zig.zon` record the
last verified standalone dependency identities; workspace builds use the
mapped local checkouts.

## Documentation

Downloads hash sink bytes during transfer and verify the published file once.
Resume rehashes only the retained prefix before continuing. Result operations
10/11 bundle each offer with its first component and up to eight subsequent
components per page, bound to one job and result generation. Existing result
operations remain available for older clients.

Detailed German technical notes from the migration are preserved in
`DOCUMENTATION.de.txt`. Source-transfer provenance is recorded in
`PROVENANCE.txt`.

## License

Original R4OS material is licensed under Apache License 2.0. See `LICENSE`
and `NOTICE`. Any repository-specific external material is documented in
`THIRD_PARTY_NOTICES.md`.


Completion and prepared restart ownership (0.78.72)
--------------------------------------------------
The worker retains a completed result, including owned reason bytes, until
the coordinator publishes it. One lock attempt runs per worker cycle;
contention yields one tick and retries without executing the work again.
A stale job is reported explicitly. Shutdown also drains a pending result.
Result fields, active ownership and admission of the next job change under
the same coordinator lock.

A fully prepared restart batch reserves its search snapshot. Other work,
including another search, is rejected before any job or result identity is
changed; the matching restart remains admissible. UpdateCenter disables a
new search and binds the restart request to the confirmed search ID and
the displayed results. A returned commit failure retains that identity for
retry, including after reopening the window. The wire contract is unchanged.

Three short host cases cover one forced completion collision, owned reason
bytes, subsequent work, UI identity checks and the combined prepare/search/
restart/retry transition. No installation is executed by these fixtures.
