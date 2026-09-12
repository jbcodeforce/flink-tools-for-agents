#!/usr/bin/env bash
# link-skills.sh — symlink every skill directory into a target skills folder
#
# Usage:
#   ./scripts/link-skills.sh <target-dir>
#
# Examples:
#   ./scripts/link-skills.sh ~/.claude/skills   # Claude Code
#   ./scripts/link-skills.sh ~/.bob/skills      # IBM Bob
#
# Each skills/<name>/ directory is linked as <target-dir>/<name>.
# Existing links are refreshed; conflicting real directories cause an error.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <target-dir>" >&2
  exit 1
fi

TARGET_DIR="${1%/}"                          # strip trailing slash
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKILLS_DIR="$REPO_ROOT/skills"

if [[ ! -d "$SKILLS_DIR" ]]; then
  echo "Error: skills directory not found at $SKILLS_DIR" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR"

linked=0
skipped=0

for skill_path in "$SKILLS_DIR"/*/; do
  [[ -d "$skill_path" ]] || continue
  skill_name="$(basename "$skill_path")"
  dest="$TARGET_DIR/$skill_name"

  if [[ -L "$dest" ]]; then
    # Refresh existing symlink (may point to a stale path)
    rm "$dest"
  elif [[ -e "$dest" ]]; then
    echo "SKIP  $skill_name — '$dest' exists and is not a symlink" >&2
    ((skipped++)) || true
    continue
  fi

  ln -s "$skill_path" "$dest"
  echo "LINK  $dest -> $skill_path"
  ((linked++)) || true
done

echo ""
echo "Done: $linked skill(s) linked, $skipped skipped."
