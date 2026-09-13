#!/bin/sh
# BigTop KVM CI guest init.
#
# This is PID 1 inside the Firecracker microVM booted by the KVM CI lane.
# Contract with the agent (`FirecrackerRuntime::spawn`):
#
#   - the kernel cmdline carries `bigtop.task=<id>` and
#     `bigtop.cmd_b64=<base64>` (base64 of the shell line to run);
#   - this script decodes `bigtop.cmd_b64` and executes it with stdout
#     wired to the serial console (`console=ttyS0`), which Firecracker
#     relays to the host so tests can assert on guest output.
#
# The base64 alphabet contains no shell metacharacters, so extracting it
# with plain parameter expansion is safe. The decoded command runs under
# `sh`; only BusyBox applets are guaranteed to exist.
set -u

mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev

CMD_B64=""
TASK_ID=""
for word in $(cat /proc/cmdline); do
    case "$word" in
        bigtop.cmd_b64=*) CMD_B64="${word#bigtop.cmd_b64=}" ;;
        bigtop.task=*) TASK_ID="${word#bigtop.task=}" ;;
    esac
done

echo "bigtop-init: task=${TASK_ID:-unknown} starting"

if [ -z "$CMD_B64" ]; then
    echo "bigtop-init: no bigtop.cmd_b64 on cmdline; idling"
    exec /bin/busybox sleep 86400
fi

echo "$CMD_B64" | /bin/busybox base64 -d > /bigtop-cmd.sh
echo "bigtop-init: running guest command"
# shellcheck disable=SC1091
/bin/busybox sh /bigtop-cmd.sh
CODE=$?
echo "bigtop-init: guest command exited with code $CODE"

# Idle so snapshot tests can capture a live VM; the host kills the
# microVM when the test is done.
/bin/busybox sleep 86400
