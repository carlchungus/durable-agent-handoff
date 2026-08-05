# Security policy

Please report vulnerabilities privately through GitHub Security Advisories for this repository. Do not include real credentials, private transcripts, or production data in an issue.

`handoff` executes coding agents and local commands, so its trust boundary matters:

- use dedicated worktrees;
- grant only repository-scoped credentials;
- review medium/high-risk transcript imports;
- do not give Pi/OhMyPi broad host secrets;
- require exact named checks for automatic merging;
- treat transcript redaction as defense in depth, not a secret scanner guarantee.

The project does not currently claim multi-tenant isolation or safe execution of hostile prompts.

## Durable state boundary

`HANDOFF_HOME` is supervisor control-plane state. Keep it outside every agent worktree and do not expose it through a worker-writable sandbox mount. On Unix, the session store rejects roots and child directories that are not owned by the supervisor user or are group/world-writable. It also rejects linked or replaced path components and revalidates the pinned session lock before each mutation. Windows uses the current user's profile ACL as this boundary and rejects reparse traversal.

Kernel locks serialize cooperating `handoff` supervisors and process exit releases them. They do not make a writable directory safe from an actively malicious process running as the same OS user: such a process can rename a path immediately after any validation. Unsandboxed same-user runtimes therefore remain inside the trusted computing base and must not receive hostile prompts or broad host access.
