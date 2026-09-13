#!/usr/bin/env bash
# Build the minimal BigTop KVM CI guest rootfs (ext4) from pinned inputs.
#
# Nothing binary is committed to the repo: this script downloads the two
# pinned artifacts below, verifies their SHA-256, and assembles a small
# ext4 image containing a static BusyBox plus `scripts/kvm/guest-init.sh`
# as `/sbin/init`.
#
# Outputs (default under ./target/kvm-guest/):
#   rootfs.ext4   the guest root filesystem
#
# Usage:
#   scripts/kvm/build-rootfs.sh [output-dir]
#
# Requirements on the build host: bash, curl, mke2fs (e2fsprogs),
# sha256sum. No root needed.
set -euo pipefail

# Pinned inputs. Re-pin by updating the hashes after verifying the new
# binaries from their official sources.
BUSYBOX_URL="https://busybox.net/downloads/binaries/1.35.0-x86_64-linux-musl/busybox"
BUSYBOX_SHA256="6e123e7f3202a8c1e9b1f94d8941580a25135382b99e8d3e34fb858bba311348"

OUT_DIR="${1:-target/kvm-guest}"
mkdir -p "$OUT_DIR"
ROOT="$OUT_DIR/rootfs-staging"
rm -rf "$ROOT"
mkdir -p "$ROOT"/{bin,sbin,proc,sys,dev,tmp}

echo "==> downloading pinned busybox"
curl -fsSL --http1.1 -o "$OUT_DIR/busybox" "$BUSYBOX_URL"
echo "$BUSYBOX_SHA256  $OUT_DIR/busybox" | sha256sum -c -
chmod +x "$OUT_DIR/busybox"

echo "==> staging rootfs"
cp "$OUT_DIR/busybox" "$ROOT/bin/busybox"
cp scripts/kvm/guest-init.sh "$ROOT/sbin/init"
chmod +x "$ROOT/sbin/init"
ln -sf /bin/busybox "$ROOT/bin/sh"

echo "==> building ext4 image"
# 64 MiB is generous for busybox + init; the guest never writes to disk
# in the KVM tests, so no journal recovery concerns.
mke2fs -q -t ext4 -d "$ROOT" -L bigtop-kvm "$OUT_DIR/rootfs.ext4" 64M

echo "==> rootfs ready: $OUT_DIR/rootfs.ext4"
sha256sum "$OUT_DIR/rootfs.ext4"
