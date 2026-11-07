#!/usr/bin/env bash
# Sets up a Raspberry Pi (Raspberry Pi OS Lite 64-bit, or any Debian with
# systemd) as a Windrow edge node for Baler (Baler#35). One role per run.
# Re-running is safe and idempotent: it installs the requested Windrow ref
# (WINDROW_REF, default main) if that isn't the commit already installed,
# rewrites the unit, and restarts the service only if either changed.
#
#   sudo bash install-edge.sh <role>
#   sudo WINDROW_REF=<branch|tag|sha> bash install-edge.sh <role>
#
# Roles:
#   bacnet   a simulated BACnet/IP device (windrow.mock_device) on this Pi's
#            LAN address, UDP 47808 - for Windrow to discover and poll
#   modbus   a simulated Modbus TCP server (windrow.mock_modbus_device) on
#            TCP 502
#   weather  a simulated weather station publishing to Baler's MQTT broker
#            (windrow.mock_weather) with an edge-node account
#   gateway  Windrow itself (windrow.api): discovers BACnet/Modbus devices on
#            this LAN and publishes them to Baler with an edge-node account
#   announce "I am here" (windrow.announce): tells a Windrow this Pi's name
#            and current address every 30 s and whenever it changes, for the
#            bacnet/modbus roles installed here, so connections that name
#            this device (device_name) follow it. Needs the Windrow's address:
#              sudo WINDROW_ANNOUNCE_TO=192.168.1.10 bash install-edge.sh announce
#            (host[:port], comma separated; default port 47900). Optional
#            WINDROW_ANNOUNCE_KEY in edge.env signs announcements.
#
# Broker settings for weather/gateway go in /etc/windrow/edge.env (created
# here with placeholders, mode 640): MQTT_HOST, MQTT_PORT (8883 = TLS),
# MQTT_USERNAME, MQTT_PASSWORD - the account from Baler's Ops -> Edge nodes,
# whose site prefix the topics must sit under.
#
# The gateway's HTTP API (port 8001) binds to 127.0.0.1: it can create
# connections and write points, and Baler manages a gateway over the MQTT
# command channel, not over HTTP. To expose it on the LAN anyway:
#   sudo WINDROW_API_HOST=0.0.0.0 bash install-edge.sh gateway
# and set WINDROW_GATEWAY_SECRET in edge.env, so a request needs the
# X-Gateway-Secret header.
#
# bacnet and gateway can't share a Pi: both bind UDP 47808 on its address.
set -euo pipefail

ROLE="${1:-}"
WINDROW_REF="${WINDROW_REF:-main}"
WINDROW_GIT=https://github.com/jacobnash/Windrow
WINDROW_API_HOST="${WINDROW_API_HOST:-127.0.0.1}"
VENV=/opt/windrow
ENV_FILE=/etc/windrow/edge.env
SVC_USER=windrow

case "$ROLE" in bacnet|modbus|weather|gateway|announce) ;; *)
  echo "usage: sudo bash $0 bacnet|modbus|weather|gateway|announce" >&2; exit 2;;
esac
[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 1; }

# A BACnet simulator and Windrow both bind UDP 47808 on this Pi's address
# (Windrow has to: bacpypes3 sends Who-Is to its own port), so the second
# would fail to start and restart forever. Refuse up front instead.
case "$ROLE" in bacnet|gateway)
  OTHER=$([ "$ROLE" = bacnet ] && echo gateway || echo bacnet)
  if systemctl is-enabled --quiet "windrow-${OTHER}.service" 2>/dev/null; then
    echo "windrow-${OTHER} is enabled here and also uses UDP 47808; one Pi runs bacnet or gateway, not both." >&2
    echo "  to switch: sudo systemctl disable --now windrow-${OTHER}" >&2
    exit 1
  fi;;
esac

echo "==> packages"
apt-get update -qq
apt-get install -y -qq python3-venv python3-pip git >/dev/null

echo "==> service user and Windrow ($WINDROW_REF)"
id -u "$SVC_USER" >/dev/null 2>&1 || useradd --system --home /var/lib/windrow --shell /usr/sbin/nologin "$SVC_USER"
[ -x "$VENV/bin/python" ] || python3 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip

# Pin the ref to a commit. `pip install --upgrade windrow @ git+...@main`
# is a no-op whenever the version string is unchanged (always 0.1.0), even
# after main has moved (Windrow#6), so compare commits instead.
if printf '%s' "$WINDROW_REF" | grep -qxE '[0-9a-f]{40}'; then
  WANT="$WINDROW_REF"
else
  # A tag's peeled ^{} line (its commit) wins over the tag object itself.
  WANT=$(git ls-remote "$WINDROW_GIT" "$WINDROW_REF" "${WINDROW_REF}^{}" \
    | awk '{sha=$1} $2 ~ /\^\{\}$/ {peeled=$1} END {print (peeled != "" ? peeled : sha)}')
  [ -n "$WANT" ] || { echo "no branch or tag '$WINDROW_REF' in $WINDROW_GIT (use a full 40-char sha for a commit)" >&2; exit 1; }
fi
# The commit pip recorded for the installed Windrow (PEP 610
# direct_url.json), or nothing if it isn't installed yet.
installed_commit() {
  "$VENV/bin/python" - <<'PY' 2>/dev/null || true
import json
from importlib.metadata import distribution
print(json.loads(distribution("windrow").read_text("direct_url.json") or "{}").get("vcs_info", {}).get("commit_id", ""))
PY
}
HAVE=$(installed_commit)
CHANGED=0
if [ "$HAVE" != "$WANT" ]; then
  echo "    ${HAVE:-not installed} -> $WANT"
  # Windrow itself, unconditionally at that commit; --no-deps so this step
  # never reinstalls dependencies that haven't changed.
  "$VENV/bin/pip" install -q --force-reinstall --no-deps "windrow @ git+${WINDROW_GIT}@${WANT}"
  CHANGED=1
else
  echo "    already at $WANT"
fi
# Dependencies: installs any the new commit added or whose pin it raised;
# leaves already-satisfied ones alone (pip's only-if-needed default).
"$VENV/bin/pip" install -q "windrow @ git+${WINDROW_GIT}@${WANT}"

mkdir -p /etc/windrow
if [ ! -f "$ENV_FILE" ]; then
  cat > "$ENV_FILE" <<ENV
# Baler broker (edge-node account from Ops -> Edge nodes). 8883 = TLS.
MQTT_HOST=timberdoodle.nash.engineering
MQTT_PORT=8883
MQTT_USERNAME=
MQTT_PASSWORD=
# weather role: must sit under the account's site prefix
WEATHER_TOPIC_PREFIX=fbf/home/weather
# gateway role: this site's id (= the broker username) and topic prefix
WINDROW_SITE_ID=
# gateway role, only if its API is exposed (WINDROW_API_HOST): requests
# then need this in X-Gateway-Secret
#WINDROW_GATEWAY_SECRET=
# Traces: off unless a collector is named here (e.g. http://<host>:4318/v1/traces)
#OTEL_EXPORTER_OTLP_ENDPOINT=
ENV
fi
chown root:"$SVC_USER" "$ENV_FILE"
chmod 640 "$ENV_FILE"

# This Pi's LAN address with its prefix length, e.g. 192.168.1.23/24 -
# BACnet needs the /24 to hear broadcasts (Who-Is).
CIDR=$(ip -o -4 addr show scope global | awk '{print $4; exit}')
[ -n "$CIDR" ] || { echo "no LAN IPv4 address found" >&2; exit 1; }

case "$ROLE" in
  bacnet)
    INSTANCE="${BACNET_INSTANCE:-$(( $(echo "${CIDR%/*}" | awk -F. '{print $4}') + 10000 ))}"
    EXEC="$VENV/bin/python -m windrow.mock_device --address ${CIDR}:47808 --instance ${INSTANCE} --name $(hostname)-sim"
    CAPS="" ;;
  modbus)
    EXEC="$VENV/bin/python -m windrow.mock_modbus_device --host 0.0.0.0 --port 502"
    CAPS="AmbientCapabilities=CAP_NET_BIND_SERVICE" ;;  # port 502 as a non-root user, no setcap on python
  weather)
    EXEC="$VENV/bin/python -m windrow.mock_weather --interval 60"
    CAPS="" ;;
  announce)
    TO="${WINDROW_ANNOUNCE_TO:-$(sed -n 's/^WINDROW_ANNOUNCE_TO=//p' "$ENV_FILE")}"
    [ -n "$TO" ] || { echo "set WINDROW_ANNOUNCE_TO=<windrow host>[:port] (or put it in $ENV_FILE)" >&2; exit 1; }
    grep -q '^WINDROW_ANNOUNCE_TO=' "$ENV_FILE" || echo "WINDROW_ANNOUNCE_TO=$TO" >> "$ENV_FILE"
    SERVICES=""
    if systemctl is-enabled --quiet windrow-bacnet.service 2>/dev/null; then
      INST=$(grep -oE -- '--instance [0-9]+' /etc/systemd/system/windrow-bacnet.service | awk '{print $2}')
      SERVICES="$SERVICES --service bacnet:47808${INST:+:$INST}"
    fi
    if systemctl is-enabled --quiet windrow-modbus.service 2>/dev/null; then
      SERVICES="$SERVICES --service modbus:502:1"
    fi
    EXEC="$VENV/bin/python -m windrow.announce --name $(hostname)${SERVICES}"
    CAPS="" ;;
  gateway)
    EXEC="$VENV/bin/python -m windrow.api --address ${CIDR}:47808 --host ${WINDROW_API_HOST} --port 8001 --mqtt-host \${MQTT_HOST} --mqtt-port \${MQTT_PORT} --site-id \${WINDROW_SITE_ID} --modbus-scan-cidr ${CIDR} --announce-port 47900 --announce-allow ${CIDR}"
    CAPS="" ;;
esac

UNIT=/etc/systemd/system/windrow-${ROLE}.service
NEW_UNIT=$(mktemp)
trap 'rm -f "$NEW_UNIT"' EXIT
cat > "$NEW_UNIT" <<UNITEOF
[Unit]
Description=Windrow edge node: ${ROLE}
Wants=network-online.target
After=network-online.target

[Service]
User=${SVC_USER}
EnvironmentFile=${ENV_FILE}
StateDirectory=windrow
WorkingDirectory=/var/lib/windrow
ExecStart=${EXEC}
${CAPS}
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNITEOF
if ! cmp -s "$NEW_UNIT" "$UNIT"; then
  install -m 644 "$NEW_UNIT" "$UNIT"
  CHANGED=1
fi

systemctl daemon-reload
if systemctl is-active --quiet "windrow-${ROLE}.service"; then
  # enable --now leaves a running service alone, so a new commit or unit
  # would only take effect at the next reboot without this.
  systemctl enable --quiet "windrow-${ROLE}.service"
  if [ "$CHANGED" = 1 ]; then
    systemctl restart "windrow-${ROLE}.service"
  fi
else
  systemctl enable --now "windrow-${ROLE}.service"
fi
echo "==> windrow-${ROLE} running ($(systemctl is-active "windrow-${ROLE}")) on Windrow $(installed_commit). Logs: journalctl -u windrow-${ROLE} -f"
case "$ROLE" in weather|gateway)
  echo "    set MQTT_USERNAME/MQTT_PASSWORD (and WINDROW_SITE_ID for gateway) in ${ENV_FILE}, then: systemctl restart windrow-${ROLE}";;
esac
