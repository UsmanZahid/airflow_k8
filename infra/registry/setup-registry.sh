#!/usr/bin/env bash
# Create the local docker registry (idempotent). Canonical kind recipe (registry:3).
set -euo pipefail

reg_name='kind-registry'
reg_port='5001'

if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
  docker run -d --restart=always -p "127.0.0.1:${reg_port}:5000" \
    --network bridge --name "${reg_name}" registry:3
  echo "created registry ${reg_name} on 127.0.0.1:${reg_port}"
else
  echo "registry ${reg_name} already running on 127.0.0.1:${reg_port}"
fi
