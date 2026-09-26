# Contributing

Thanks for helping. Changes come in through pull requests from forks:

1. Fork the repo and create a branch in your fork.
2. Make your change and add tests where it makes sense.
3. Open a pull request against `main`.

## Rules on this repository

- `main` can only change through a pull request. Force-pushes and deletion
  are blocked.
- Every PR needs an approving review from the code owner
  ([@damiensmith1](https://github.com/damiensmith1)), all review threads
  resolved, and passing lint and tests (gofmt, vet, unit and integration). A new push after approval needs a fresh approval.
- Only the maintainer can create branches or tags in this repository.
- CI on PRs from outside contributors runs only after the maintainer
  approves it. Workflows get a read-only token.

Never commit secrets, `.env` files or personal data. Please report
security issues privately; see [SECURITY.md](SECURITY.md).
