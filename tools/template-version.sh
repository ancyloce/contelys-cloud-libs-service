#!/usr/bin/env bash
# Checks that the service template requires the library at the library's latest version tag, so
# that a domain service copied from the template starts on the current library and the template
# never falls behind it (ADR 0034). A tag is v<major>.<minor>.<patch> in the library's own
# repository (ADR 0069); a pre-release such as v1.2.0-rc1 is none. A tag on HEAD itself does not
# count, because the template can only require a version that precedes its own commit. Before the
# first tag there is nothing to compare: the check says what the template requires and passes, as
# it does in a clone without tags.
#
# Usage, from the root of the library: bash tools/template-version.sh
set -euo pipefail

module=$(GOWORK=off go list -m)
# Empty when the template does not require the library's module path.
required=$(cd template && GOWORK=off go list -m -f '{{.Version}}' "$module" 2>/dev/null || true)
if [ -n "$required" ]; then
  wants="requires $module at $required"
else
  wants="does not require $module"
fi
latest=$(git tag --list 'v*' --merged HEAD --no-contains HEAD --sort=-v:refname |
  grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -n 1 || true)
if [ -z "$latest" ]; then
  echo "The library has no version tag yet; the template $wants."
  exit 0
fi
if [ "$required" != "$latest" ]; then
  echo "The library's latest tag is $latest, but the template $wants:" \
    "run 'GOWORK=off go get $module@$latest' in template/." >&2
  exit 1
fi
echo "The template requires $module at its latest tag, $latest."
