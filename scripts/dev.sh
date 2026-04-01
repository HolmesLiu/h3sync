#!/usr/bin/env bash
set -euo pipefail

cp -n .env.example .env || true

go mod tidy

go run ./cmd/server
