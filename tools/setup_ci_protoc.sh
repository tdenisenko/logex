#!/usr/bin/env bash
# Keep the compiler selected by the former setup-protoc@v3 action. Download
# directly from the same upstream release without depending on a Node runtime.
set -euo pipefail

version=23.4
case "${RUNNER_OS:-}/${RUNNER_ARCH:-}" in
  Linux/X64)
    platform=linux-x86_64
    sha256=0502f286ac9ed860b629a7965a14527b1f2dd131e4283fa23c2d7f184672aa9a
    ;;
  macOS/ARM64)
    platform=osx-aarch_64
    sha256=8c7afae8626b6811e7b5897d16d940c2dbf50b1e135ed958a01db6566bdda726
    ;;
  macOS/X64)
    platform=osx-x86_64
    sha256=07e5fdcf1b0708d3367dc5e6eb8d135de7e407d75316c93155cfd8ab362eec80
    ;;
  *)
    printf 'Unsupported CI platform: %s/%s\n' "${RUNNER_OS:-unset}" "${RUNNER_ARCH:-unset}" >&2
    exit 1
    ;;
esac

: "${RUNNER_TEMP:?RUNNER_TEMP must identify the job temporary directory}"
: "${GITHUB_PATH:?GITHUB_PATH must identify the runner path file}"
install_dir=$(mktemp -d "$RUNNER_TEMP/logex-protoc-$version.XXXXXX")
archive="$install_dir/protoc.zip"
curl --fail --location --silent --show-error --retry 3 \
  --connect-timeout 15 --max-time 120 --retry-max-time 180 \
  --output "$archive" \
  "https://github.com/protocolbuffers/protobuf/releases/download/v$version/protoc-$version-$platform.zip"

# Authenticate the pinned archive before extracting or executing its contents.
printf '%s  %s\n' "$sha256" "$archive" | shasum -a 256 --check
unzip -q "$archive" -d "$install_dir"
actual_version=$("$install_dir/bin/protoc" --version)
if [[ "$actual_version" != "libprotoc $version" ]]; then
  printf 'Unexpected protoc version: %s\n' "$actual_version" >&2
  exit 1
fi
printf '%s\n' "$install_dir/bin" >> "$GITHUB_PATH"
printf '%s\n' "$actual_version"
