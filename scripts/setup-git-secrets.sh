#!/usr/bin/env bash
#
# Set up git-secrets for this repository: install the commit hooks and register the
# secret patterns relevant to AnVIL / Terra / Google Cloud HTTP captures, so a secret
# can never be accidentally committed. This is the commit-time backstop that pairs with
# scripts/scrub_har.py (which redacts captures before they are shared/stored).
#
# Usage (run once per clone):
#     ./scripts/setup-git-secrets.sh
#
# Requires git-secrets: https://github.com/awslabs/git-secrets
#     macOS:  brew install git-secrets
#
# Idempotent: safe to re-run; it resets the managed [secrets] config each time.
#
set -euo pipefail

if ! command -v git-secrets >/dev/null 2>&1; then
  echo "ERROR: git-secrets not found. Install it first (macOS: 'brew install git-secrets')." >&2
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"

# Install the git-secrets hooks (pre-commit, commit-msg, prepare-commit-msg).
git secrets --install --force

# Reset our managed patterns so re-running stays clean and idempotent.
git config --remove-section secrets >/dev/null 2>&1 || true

# AWS provider patterns + AWS example-key allowances (parity with related repos).
git secrets --register-aws

# --- Project-specific patterns: Google / Terra / AnVIL ---
git config --add secrets.patterns 'ya29\.[0-9A-Za-z_-]+'                                  # Google OAuth access tokens
git config --add secrets.patterns '1//[0-9A-Za-z_-]{20,}'                                 # Google OAuth refresh tokens
git config --add secrets.patterns 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+'    # JWTs (id/access tokens, passports)
git config --add secrets.patterns 'AIza[0-9A-Za-z_-]{35}'                                 # Google API keys
git config --add secrets.patterns 'BEGIN [A-Z ]*PRIVATE KEY'                              # private keys (e.g. service-account)
git config --add secrets.patterns '[Xx]-[Gg]oog-[Ss]ignature(=|%3D)[0-9A-Za-z%_-]{20,}'  # unredacted signed-URL signatures

echo
echo "git-secrets configured for this clone. Registered patterns:"
git secrets --list
