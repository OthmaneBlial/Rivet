# Security policy

Rivet is an early local-first alpha. It is not a hosted service, a secret
manager replacement, or a guarantee of production-safe execution. Treat build
steps, remote agents, extensions, migration drafts, provider webhooks, and
container declarations as privileged capabilities.

## Reporting a vulnerability

Please use a [private GitHub Security Advisory](https://github.com/OthmaneBlial/Rivet/security/advisories/new)
instead of opening a public issue. Include the affected commit, operating
system, reproduction steps, impact, and a minimal fixture. Do not attach live
credentials or private production data.

If private advisories are unavailable, open an issue containing only the fact
that a security report needs a private channel; do not publish exploit details.

## Local safety boundaries

- Keep secrets in the encrypted credential vault or private token files.
- Use exact CORS origins and authenticated transport for non-loopback binds.
- Use disposable repositories and loopback-only protocol fixtures.
- Review every generated migration draft before adopting it.
- Keep extensions on explicit permissions and bounded inputs.
- Do not assume the unsigned alpha bundle is suitable for production rollout.

The repository's implementation notes and local validation scripts are useful
evidence, but they do not replace an environment-specific threat model or
operational review.
