#!/usr/bin/env bash
if [[ -z "$SUDO_USER" ]]; then
    echo "This script must be run with sudo."
    exit 1
fi
modprobe null_blk
chown $SUDO_USER:$SUDO_USER /dev/nullb*
