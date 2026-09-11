#!/usr/bin/env bash
# BigTop v0.3 — one-time host network setup, run once per node as root.
#
# What it does:
#   1. Enables IPv4 forwarding (persisted via /etc/sysctl.d).
#   2. Adds an nftables NAT table that masquerades outbound traffic from
#      the BigTop pod CIDR (default 172.28.0.0/16, override with
#      BIGTOP_POD_CIDR), so microVM guests can reach the outside world.
#
# What it does NOT do:
#   - It never touches per-task state. Tap devices are created/destroyed
#     by the agent itself (which needs CAP_NET_ADMIN or root).
#   - It does not install a DHCP server: guests get static IPs on the
#     kernel cmdline (see SPEC.md "Networking (v0.3)").
#
# Requirements: nftables (the `nft` binary). No firewalld/iptables
# coexistence is attempted — pick one packet filter per host.
set -euo pipefail

CIDR="${BIGTOP_POD_CIDR:-172.28.0.0/16}"

if [ "$(id -u)" -ne 0 ]; then
    echo "setup-nat.sh: must run as root" >&2
    exit 1
fi
command -v nft >/dev/null || {
    echo "setup-nat.sh: 'nft' not found — install nftables first" >&2
    exit 1
}

echo "setup-nat.sh: enabling IPv4 forwarding"
sysctl -w net.ipv4.ip_forward=1 >/dev/null
printf 'net.ipv4.ip_forward=1\n' > /etc/sysctl.d/99-bigtop.conf

echo "setup-nat.sh: masquerading $CIDR"
nft list table inet bigtop >/dev/null 2>&1 || nft add table inet bigtop
nft list chain inet bigtop postrouting >/dev/null 2>&1 \
    || nft add chain inet bigtop postrouting \
        '{ type nat hook postrouting priority 100; policy accept; }'
# Idempotent: flush our own chain, then install the single masquerade rule.
nft flush chain inet bigtop postrouting
nft add rule inet bigtop postrouting ip saddr "$CIDR" masquerade

echo "setup-nat.sh: done — guests in $CIDR can now reach the network"
