#!/usr/bin/env bash
#
# Start Confluent Schema Registry under Apple's `container` CLI, wired to the
# host-native Kafka broker via its DOCKER listener (advertised as
# 192.168.64.1:19092 in server.properties).
#
# Note: 192.168.64.1 is the default apple-container subnet gateway. It's stable
# for the default network but not guaranteed across a full networking reset —
# if containers ever land on a different subnet, re-check:
#   container network inspect default
set -euo pipefail

NAME="sr79"
PORT=8081

# Remove any previous instance so re-running is idempotent.
container rm -f "$NAME" >/dev/null 2>&1 || true

container run -d -p "${PORT}:${PORT}" --name "$NAME" \
   -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
   -e SCHEMA_REGISTRY_LISTENERS="http://0.0.0.0:${PORT}" \
   -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS='PLAINTEXT://192.168.64.1:19092' \
   confluentinc/cp-schema-registry:7.9.10 >/dev/null

# Apple `container` gives each container its own IP on the vmnet subnet; the
# published -p mapping is a Docker-ism, so reach SR at <container-ip>:PORT.
# Poll until the container reports an IPv4 address.
echo "Starting $NAME ..."
IP=""
for _ in $(seq 1 30); do
  # Match the ipv4Address field specifically — a plain 192.168.* grep would
  # also catch the gateway IP embedded in the bootstrap-servers env var.
  IP="$(container inspect "$NAME" 2>/dev/null \
        | grep '"ipv4Address"' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
  [[ -n "$IP" ]] && break
  sleep 1
done

if [[ -z "$IP" ]]; then
  echo "WARN: could not determine container IP; check 'container ls'." >&2
else
  echo
  echo "Schema Registry reachable at: http://${IP}:${PORT}"
  echo "  health:  curl -s http://${IP}:${PORT}/subjects"
  echo
fi

echo "Following logs (Ctrl-C to detach; container keeps running):"
exec container logs --follow "$NAME"
