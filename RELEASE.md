# pskr-mqtt-cache Release Process

This document describes how to build images locally for development/testing and how to trigger and monitor releases via GitHub Actions using the GitHub CLI (`gh`).

---

## 1. Local Development & Testing ("edge" Image)

For local development and testing, you can build a local Docker image tagged with `edge`:

```bash
# From the repository root:
./docker/build-image.sh

# Or from inside the docker/ directory:
cd docker && ./build-image.sh
```

- When not on a Git tag, the script automatically falls back to the `edge` tag and builds `komacke/pskr-mqtt-cache:edge`.
- **Options & Environment Variables**:
  - `-n`: Build with `--no-cache`.
  - `-m`: Multi-platform build (`linux/amd64`, `linux/arm64`) using Buildx and pushes to Docker Hub.
  - `FORCE=true`: Bypass the uncommitted local edits check during local builds (GitHub Actions automatically bypasses this via `CI=true`).

---

## 2. Tag Naming Convention

This repository uses the `number.number` format for releases (e.g., `1.19`, `1.20`).
- **Do not use a `v` prefix** (use `1.20`, not `v1.20`).
- The build script automatically tags stable version tags with `:latest` on Docker Hub in addition to the version tag.

---

## 3. Triggering a Release via GitHub CLI (`gh`)

### Prerequisites

Ensure the GitHub CLI is authenticated:

```bash
gh auth status
```

### Triggering the Release Workflow

Trigger the release workflow manually using `workflow_dispatch`:

```bash
# Trigger release build for version 1.20:
gh workflow run release.yml -f tag_name=1.20

# Optional: skip Docker Hub image build/push:
gh workflow run release.yml -f tag_name=1.20 -f build_docker=false

# Interactive mode (prompts for inputs):
gh workflow run
```

---

## 4. Monitoring the Workflow

```bash
# Watch the latest workflow run in real time:
gh run watch

# List recent runs for the release workflow:
gh run list --workflow=release.yml -L 5

# View run details or logs:
gh run view <RUN_ID>
gh run view <RUN_ID> --log
gh run view <RUN_ID> --log-failed
```

---

## 5. Required Repository Secrets

The Docker build job requires the following GitHub repository secrets:

| Secret Name | Description |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub username (e.g., `komacke`) |
| `DOCKERHUB_TOKEN` | Docker Hub Personal Access Token (PAT) with write permissions |

---

## 6. Verified Commits & Tags (SSH Signing Keys)

If your local git environment is configured to sign commits or tags with SSH (`commit.gpgsign=true` / `tag.gpgsign=true`), GitHub requires that your public key be registered specifically as a **Signing Key** (rather than only an Authentication Key):

1. Check your public key:
   ```bash
   cat ~/.ssh/git-signing.pub
   ```
2. Go to **GitHub Settings -> [SSH and GPG keys](https://github.com/settings/keys)**.
3. Click **New SSH Key**.
4. In the **Key type** dropdown, select **Signing Key**.
5. Paste your public key and save.

*(Alternatively, run `gh auth refresh -h github.com -s admin:ssh_signing_key` and then `gh ssh-key add ~/.ssh/git-signing.pub --type signing`)*.

Once added as a signing key, GitHub will mark your releases, tags, and commits with the green **Verified** badge.
