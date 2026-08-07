# Security policy

Please report vulnerabilities privately through GitHub Security Advisories for this repository. Do not include real credentials, private transcripts, or production data in an issue.

`handoff` executes coding agents and local commands, so what it trusts matters:

- use dedicated worktrees;
- grant only repository-scoped credentials;
- review medium/high-risk transcript imports;
- do not give Pi/OhMyPi broad host secrets;
- require exact named checks for automatic merging;
- treat transcript redaction as defense in depth, not a secret scanner guarantee.

The project does not currently claim multi-tenant isolation or safe execution of hostile prompts.

## Protect the state files

`HANDOFF_HOME` contains the supervisor's state. Keep it outside every agent worktree and do not expose it through a worker-writable sandbox mount. On Unix, the session store rejects roots and child directories that are not owned by the supervisor user or are group/world-writable. It also rejects linked or replaced path components and rechecks the open session lock before each change. On Windows, it checks the current user's folder permissions and rejects paths redirected through links.

Kernel locks serialize cooperating `handoff` supervisors and process exit releases them. They do not make a writable directory safe from an actively malicious process running as the same OS user: such a process can rename a path immediately after any validation. An unsandboxed runtime using the same OS account must therefore be trusted and must not receive hostile prompts or broad host access.
