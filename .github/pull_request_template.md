## Summary

Describe the user-visible change and the problem it solves.

## Scope and compatibility

- [ ] This change is limited to the stated workflow.
- [ ] Jenkins compatibility claims are backed by a fixture or live evidence.
- [ ] Unsupported or experimental behavior remains explicitly documented.
- [ ] No secrets, private keys, customer data, or real hostnames are included.

## Validation

List the exact commands run and summarize their results.

```sh
cargo fmt --all -- --check
cargo test --workspace
./scripts/local-progress-check.sh
```

For packaging, desktop integration, storage boundaries, or deployment changes,
also include the relevant result from:

```sh
./scripts/local-release-check.sh
```

## Evidence

- Screenshots or demo link, when the UI changes:
- Fixture or reproduction path, when behavior changes:
- Remaining unverified boundary:

## Checklist

- [ ] Documentation and roadmap status are updated when behavior changes.
- [ ] Tests cover the new behavior or the PR explains why coverage is not
      applicable.
- [ ] This PR does not add GitHub Actions or other hosted CI unless explicitly
      requested for the project.
