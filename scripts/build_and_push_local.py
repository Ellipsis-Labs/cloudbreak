#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Build Cloudbreak with Nix inside Docker and publish to Ellipsis's ECR."""

import argparse
import os
import platform
import re
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILDER = "cloudbreak-container"


def run(command, *, capture=False, check=True, input=None):
    print(f"+ {shlex.join(command)}", flush=True)
    return subprocess.run(
        command, cwd=ROOT, text=True, capture_output=capture,
        check=check, input=input,
    )


def image_tag(registry, repository, dev):
    sha = run(["git", "rev-parse", "--short", "HEAD"], capture=True).stdout.strip()
    tree = run(["git", "rev-parse", "HEAD^{tree}"], capture=True).stdout.strip()[:12]
    dirty = bool(run(["git", "status", "--porcelain", "--untracked-files=normal"], capture=True).stdout)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    tag = f"{'dev-' if dev else ''}{timestamp}-{sha}-{tree}{'-dirty' if dirty else ''}"
    return f"{registry}/{repository}:{tag}"


def build_command(image, arm_only, dry_run):
    if dry_run:
        architecture = platform.machine().lower()
        if architecture not in ("arm64", "aarch64", "amd64", "x86_64"):
            raise ValueError(f"Unsupported local architecture: {architecture}")
        platforms = "linux/arm64" if arm_only or architecture in ("arm64", "aarch64") else "linux/amd64"
    else:
        platforms = "linux/arm64" if arm_only else "linux/arm64,linux/amd64"
    return [
        "docker", "buildx", "build", "--builder", BUILDER,
        "--platform", platforms, "--file", str(ROOT / ".docker/Dockerfile.local"),
        "--tag", image,
        "--load" if dry_run else "--push", str(ROOT),
    ]


def setup_ecr(account, region, repository, registry):
    # Avoid accidentally creating the repository in the caller's other AWS account.
    caller = run(["aws", "sts", "get-caller-identity", "--region", region, "--query", "Account", "--output", "text"], capture=True).stdout.strip()
    if caller != account:
        raise ValueError(f"AWS credentials belong to {caller}; select a profile/role for {account} with AWS_PROFILE.")
    aws = ["aws", "ecr", "--region", region]
    result = run(aws + ["describe-repositories", "--registry-id", account, "--repository-names", repository], capture=True, check=False)
    if result.returncode:
        if "RepositoryNotFoundException" not in result.stderr:
            raise RuntimeError(result.stderr.strip())
        run(aws + ["create-repository", "--repository-name", repository,
                   "--image-tag-mutability", "IMMUTABLE",
                   "--image-scanning-configuration", "scanOnPush=true"])
    password = run(aws + ["get-login-password"], capture=True).stdout.strip()
    run(["docker", "login", "--username", "AWS", "--password-stdin", registry], input=password)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm-only", action="store_true", help="Build only linux/arm64")
    parser.add_argument("--dry-run", action="store_true", help="Build and load one architecture locally without contacting AWS")
    parser.add_argument("--plan", action="store_true", help="Print the build command without building or contacting AWS")
    parser.add_argument("--dev", action="store_true", help="Prefix the image tag with dev-")
    parser.add_argument("--repository", default="cloudbreak", help="ECR repository name (default: cloudbreak)")
    args = parser.parse_args()

    account = os.environ.get("AWS_ACCOUNT_ID", "829210487188")
    region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    repository = args.repository
    if not re.fullmatch(r"\d{12}", account):
        parser.error("AWS_ACCOUNT_ID must contain 12 digits")
    if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-\d+", region):
        parser.error("AWS_REGION must be an AWS region name")
    if len(repository) < 2 or len(repository) > 256 or not re.fullmatch(r"(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*", repository):
        parser.error("Invalid ECR repository name")
    registry = f"{account}.dkr.ecr.{region}.amazonaws.com"
    image = image_tag(registry, repository, args.dev)
    command = build_command(image, args.arm_only, args.dry_run)
    if args.plan:
        print(shlex.join(command))
        return
    if not args.dry_run:
        setup_ecr(account, region, repository, registry)
    builder = run(["docker", "buildx", "inspect", BUILDER], capture=True, check=False)
    if builder.returncode:
        run(["docker", "buildx", "create", "--name", BUILDER, "--driver", "docker-container"])
    run(command)
    print(f"{'Loaded' if args.dry_run else 'Pushed'}: {image}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr.strip(), file=sys.stderr)
        sys.exit(1)
