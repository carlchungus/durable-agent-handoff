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
