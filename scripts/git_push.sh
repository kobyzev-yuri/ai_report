#!/bin/bash
# Push в GitHub, подставляя GITHUB_TOKEN из config.env (config.env в .gitignore).
# Запуск из корня проекта: ./scripts/git_push.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

if [ -f "config.env" ]; then
  set -a
  source "config.env"
  set +a
fi

if [ -z "$GITHUB_TOKEN" ]; then
  echo "GITHUB_TOKEN не задан. Добавьте в config.env:"
  echo "  GITHUB_TOKEN=ghp_..."
  exit 1
fi

REPO="${GITHUB_REPO:-kobyzev-yuri/ai_report}"
BRANCH="${1:-main}"

git remote set-url origin "https://${GITHUB_TOKEN}@github.com/${REPO}.git"
git push origin "$BRANCH"
