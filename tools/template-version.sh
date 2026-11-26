#!/usr/bin/env bash
# Checks that the service template requires the library at the library's latest version tag, so
# that a domain service copied from the template starts on the current library and the template
# never falls behind it (ADR 0034). A tag is libs/service/v<major>.<minor>.<patch>; a pre-release
# such as libs/service/v1.2.0-rc1 is none. A tag on HEAD itself does not count, because the
# template can only require a version that precedes its own commit. Without a tag yet, it says
# which version the template requires and passes. A clone without tags passes the same way.
#
# Usage, from libs/service: bash tools/template-version.sh
set -euo pipefail

module=github.com/ancyloce/contelys-cloud/libs/service
required=$(cd template && GOWORK=off go list -m -f '{{.Version}}' "$module")
latest=$(git tag --list 'libs/service/v*' --merged HEAD --no-contains HEAD --sort=-v:refname |
  grep -E '^libs/service/v[0-9]+\.[0-9]+\.[0-9]+$' | head -n 1 || true)
if [ -z "$latest" ]; then
  echo "The library has no version tag yet; the template requires it at $required."
  exit 0
fi
latest=${latest#libs/service/}
if [ "$required" != "$latest" ]; then
  echo "The template requires the library at $required, but the library's latest tag is $latest:" \
    "run 'GOWORK=off go get $module@$latest' in template/." >&2
  exit 1
fi
echo "The template requires the library at its latest tag, $latest."
