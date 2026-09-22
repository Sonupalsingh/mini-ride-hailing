#!/usr/bin/env bash
# Run this ONCE before deploying, on any fresh Kubernetes cluster.
# It fixes/checks the two environmental prerequisites this project needs
# that are outside the app's own YAML: a default StorageClass, and (on
# RHEL/CentOS nodes) correct unqualified-image registry resolution.
set -euo pipefail

echo "==> Checking for a default StorageClass..."
if kubectl get storageclass 2>/dev/null | grep -q "(default)"; then
  echo "    A default StorageClass already exists. Skipping."
else
  echo "    None found — installing local-path-provisioner..."
  kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.30/deploy/local-path-storage.yaml
  kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
  echo "    Installed and set local-path as default."
  echo "    Waiting for the provisioner to be ready..."
  kubectl -n local-path-storage rollout status deployment local-path-provisioner --timeout=60s
fi

echo ""
echo "==> Checking node registry resolution (RHEL/CentOS only)..."
if [ -f /etc/containers/registries.conf ]; then
  FIRST_REGISTRY=$(grep -oP '(?<=unqualified-search-registries = \[)[^]]*' /etc/containers/registries.conf 2>/dev/null | head -1 | tr -d '"' | cut -d',' -f1 | xargs)
  if [ "$FIRST_REGISTRY" = "registry.redhat.io" ] || [ "$FIRST_REGISTRY" = "registry.access.redhat.com" ]; then
    echo "    WARNING: /etc/containers/registries.conf resolves unqualified"
    echo "    image names against Red Hat's registry FIRST. Every image in"
    echo "    this project's manifests is already prefixed with docker.io/"
    echo "    as a workaround, so this should not block the deploy — but if"
    echo "    you add any new image reference without a registry prefix,"
    echo "    it may fail with a 'login to the Red Hat Registry' error."
    echo "    To fix permanently, edit that file so docker.io comes first"
    echo "    in unqualified-search-registries, e.g.:"
    echo '      unqualified-search-registries = ["docker.io", "quay.io"]'
  else
    echo "    Looks fine (docker.io is not shadowed by registry.redhat.io)."
  fi
else
  echo "    Not a RHEL/CentOS node (no /etc/containers/registries.conf) — skipping."
fi

echo ""
echo "==> Checking container runtime..."
RUNTIME=$(kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}')
echo "    Detected: $RUNTIME"
case "$RUNTIME" in
  cri-o*)
    echo "    This is CRI-O. If you build the ride-app image locally with"
    echo "    'docker build', import it with skopeo before deploying:"
    echo "      skopeo copy docker-daemon:mini-ride-app:1.0 containers-storage:mini-ride-app:1.0"
    ;;
  containerd*)
    echo "    This is containerd. If you build the ride-app image locally"
    echo "    with 'docker build', import it with ctr:"
    echo "      docker save mini-ride-app:1.0 -o /tmp/ride-app.tar"
    echo "      ctr -n k8s.io images import /tmp/ride-app.tar"
    ;;
  *)
    echo "    Runtime not CRI-O or containerd — if it's Docker itself, a"
    echo "    locally built image should already be visible; otherwise"
    echo "    check that runtime's own image-import mechanism."
    ;;
esac

echo ""
echo "==> Prerequisites check complete. You can now run:"
echo "    kubectl apply -f k8s/00-namespace.yaml"
echo "    kubectl apply -f k8s/01-zookeeper.yaml"
echo "    kubectl apply -f k8s/02-kafka.yaml"
echo "    (wait for zookeeper-0 to be Running, then:)"
echo "    kubectl apply -f k8s/03-create-topic-job.yaml"
echo "    kubectl apply -f k8s/04-ride-app.yaml"
