# ADR 0003: Deterministic one-way import from v1 ledgers

- Status: accepted
- Date: 2026-08-07

## Context

Supervisor v2 deliberately removes the independently writable Workflow,
Session, Activity, run-manifest, and provider-health stores. Maintaining v1 and
v2 writers would preserve the crash windows v2 exists to remove. Cross-ledger
wall-clock timestamps also cannot reconstruct a trustworthy global v1 order.

## Decision

Migration is a one-way import, implemented by `Store.ImportV1`.

The importer:

1. reads only legacy Workflow `events.jsonl` files in sorted path order;
2. validates and replays each Workflow by its own sequence, never from a
   replaceable `state.json` snapshot;
3. hashes every imported workflow path and byte stream into one source digest;
4. normalizes workflow history against a cloned v2 projection; and
5. appends one `legacy.imported` Supervisor transaction containing deterministic
   qualified identities and per-file checksums.

The source bytes are never modified. Repeating the same command key and source
digest returns the original receipt. Attempting to import the same source under
a different key is rejected. Changing source bytes changes the command digest
and conflicts with reuse of the original key.

Legacy completed→reopened history is normalized into an immutable completed
Activity/Result generation followed by a continuation generation. It is never
imported as a mutable completed Node returning to ready. Session and Activity
ledgers are not replayed, so exact native Session/Activity recovery is
unsupported: normalized Sessions are explicitly marked `imported_unresolved`,
their continuations are held for human promotion, and the importer does not
scrape output or invent a resumable provider identity.

Legacy cross-store chronology is not guessed. Workflow-history completion
markers receive deterministic import-qualified Activity/Attempt identities and
provenance; no v1 Session, Activity, or Attempt ledger is replayed. Imported
Attempts do not receive a live writer Lease or publication authority.
Publication evidence is historical until independently verified in v2.

After a successful import, the Supervisor journal is the only execution state.
There is no dual-write or fallback read path.

## Consequences

- Migration is repeatable and auditable from immutable checksums.
- A live v1 writer must be quiesced before promotion; import does not grant a v2
  Lease beside ambiguous legacy process authority.
- Some v1 facts remain explicitly unresolved instead of being guessed.
- Backward readability is preserved because legacy source bytes remain in place
  and use their original schema; non-workflow ledgers are outside the import
  authority and are not replayed.

## Rejected alternatives

- Merging legacy ledgers by timestamp.
- Reading whichever v1 snapshot happens to exist.
- Running v1 and v2 orchestration indefinitely behind a compatibility switch.
- Guessing Session or Activity identities from recursive JSON/output searches.
