#!/usr/bin/env bash
# Verify that the address a firewall pins to your reverse proxy still belongs to
# that proxy, and to nothing else.
#
# WHY THIS EXISTS. Running the bridge behind a proxy usually means two rules: let
# the proxy's address reach the port, drop everything else. That makes a single
# IP load-bearing for both reachability and (under --trust-proxy) the right to
# speak for any client address. On a Docker network with no fixed IPAM the
# address is dynamic, so a recreated container can quietly take it:
#
#   the proxy moves off the pinned address   -> the route dies. It times out
#     while every other route looks healthy, which reads like anything but a
#     firewall pin.
#   another container lands ON the pinned address -> worse, and silent. It
#     inherits the proxy's reach, and a symptom check never sees it: the route
#     keeps answering normally right up until the squatter uses what it got.
#
# The second case is why this checks the cause rather than the symptom.
# --trust-proxy-secret defends against it (the squatter cannot produce the
# edge's header), but that is a second lock, not a reason to leave this
# undetected.
#
# Read-only. Needs docker access, not root -- it reads the live container/address
# mapping, which is the fact that actually drifts. It does NOT read the firewall:
# pass the pinned address you configured there.
#
#   ./check-edge-pin.sh --network coolify --container coolify-proxy --addr 10.0.1.13
#
# Exit: 0 pinned address still held by the expected container
#       1 pinned address unclaimed -- route is broken or about to be
#       2 pinned address held by SOMETHING ELSE, or the proxy has moved
set -euo pipefail

NETWORK=""; CONTAINER=""; ADDR=""; QUIET=0

die() { echo "check-edge-pin: $*" >&2; exit 2; }
say() { [ "$QUIET" -eq 1 ] || echo "$*"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --network)   NETWORK="${2:-}"; shift 2 ;;
    --container) CONTAINER="${2:-}"; shift 2 ;;
    --addr)      ADDR="${2:-}"; shift 2 ;;
    --quiet|-q)  QUIET=1; shift ;;
    -h|--help)   sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)           die "unknown argument: $1" ;;
  esac
done
[ -n "$NETWORK" ] && [ -n "$CONTAINER" ] && [ -n "$ADDR" ] \
  || die "usage: $0 --network NET --container NAME --addr IP [--quiet]"
command -v docker >/dev/null || die "docker not found"

# name<TAB>ip, one container per line. IPv4Address carries a /mask; strip it.
MAP="$(docker network inspect "$NETWORK" \
        --format '{{range .Containers}}{{.Name}}	{{.IPv4Address}}{{println}}{{end}}' 2>/dev/null \
      | sed 's#/[0-9]*$##' | grep -v '^[[:space:]]*$')" \
  || die "cannot inspect network '$NETWORK' (does it exist? is docker reachable?)"

holder="$(printf '%s\n' "$MAP" | awk -F'\t' -v a="$ADDR" '$2 == a {print $1; exit}')"
current="$(printf '%s\n' "$MAP" | awk -F'\t' -v c="$CONTAINER" '$1 == c {print $2; exit}')"

# A container with no IPAMConfig on this network was not given a static address,
# so docker may hand it a different one whenever it is recreated -- the
# precondition for any of this drifting. (Ask the CONTAINER, not the network: a
# network almost always has an IPAM subnet, which says nothing about whether its
# members are pinned.) Worth saying once, in the healthy case.
static="$(docker inspect "$CONTAINER" \
            --format "{{with index .NetworkSettings.Networks \"$NETWORK\"}}{{.IPAMConfig}}{{end}}" \
          2>/dev/null || true)"
case "$static" in ''|'<nil>') dynamic=1 ;; *) dynamic=0 ;; esac

if [ -z "$current" ]; then
  echo "CRITICAL: container '$CONTAINER' is not on network '$NETWORK'." >&2
  [ -n "$holder" ] && echo "          '$holder' currently holds the pinned $ADDR." >&2
  exit 2
fi

if [ "$holder" = "$CONTAINER" ]; then
  say "OK: $ADDR is held by '$CONTAINER' on '$NETWORK'."
  [ "$dynamic" -eq 1 ] && say "    Note: '$CONTAINER' has no static address on '$NETWORK', so docker" \
                       && say "    may give it another whenever it is recreated. Hence this check."
  exit 0
fi

if [ -z "$holder" ]; then
  echo "WARNING: nothing holds the pinned $ADDR." >&2
  echo "         '$CONTAINER' is now at $current, so the firewall rule points at a" >&2
  echo "         dead address: the route will time out while everything else looks" >&2
  echo "         healthy. Repoint the rule (and the backend url) at $current." >&2
  exit 1
fi

echo "CRITICAL: the pinned $ADDR is held by '$holder', NOT '$CONTAINER'." >&2
echo "          '$CONTAINER' is at $current." >&2
echo "          '$holder' has inherited whatever the firewall grants $ADDR --" >&2
echo "          reach into the bridge, and under --trust-proxy the ability to" >&2
echo "          forge a client address unless --trust-proxy-secret is set." >&2
echo "          Repoint the firewall rule at $current and confirm what '$holder' is." >&2
exit 2
