#!/usr/bin/env bash
# Wire the running kind cluster to the local registry: per-node hosts.toml, connect the
# registry to the kind network, and publish the local-registry-hosting ConfigMap.
set -euo pipefail

CLUSTER="${1:-etl}"
reg_name='kind-registry'
reg_port='5001'
registry_dir="/etc/containerd/certs.d/localhost:${reg_port}"

for node in $(kind get nodes --name "${CLUSTER}"); do
  docker exec "${node}" mkdir -p "${registry_dir}"
  printf '[host."http://%s:5000"]\n' "${reg_name}" | \
    docker exec -i "${node}" cp /dev/stdin "${registry_dir}/hosts.toml"
done

if [ "$(docker inspect -f '{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect kind "${reg_name}"
  echo "connected ${reg_name} to the kind network"
fi

kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF
echo "registry wired to cluster ${CLUSTER}"
