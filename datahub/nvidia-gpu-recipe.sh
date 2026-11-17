#!/usr/bin/env bash
# Data Hub recipe (PRE_SERVICE_DEPLOYMENT, runs as root) for a p4d.24xlarge compute group on the
# 7.3.2 Data Engineering template. UNTESTED: written and registered, not yet run on a GPU node.
# The default 7.3.2 image (RHEL 9.6) has no dnf repos and no NVIDIA bits, so:
#   - NVIDIA driver 615.71.10 from the .run installer, open kernel modules, built against the
#     preinstalled kernel-devel (the RPM route needs RHEL repos the image doesn't have);
#   - Fabric Manager 615.71.10 from NVIDIA's redist tarball (p4d = 8x A100 on NVSwitch: without
#     a running FM of the same version, CUDA fails to initialise);
#   - persistence daemon, nouveau blacklist;
#   - kernel args for cgroup v1 (YARN GPU isolation does not support cgroup v2). Takes effect on
#     the next boot only: reboot the instance after the scale-up completes.
# Exits 0 without changes on hosts with no NVIDIA PCI device, so it is safe on any group.
set -euo pipefail

DRIVER_VERSION=615.71.10
DRIVER_URL="https://us.download.nvidia.com/tesla/${DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DRIVER_VERSION}.run"
FM_URL="https://developer.download.nvidia.com/compute/nvidia-driver/redist/fabricmanager/linux-x86_64/fabricmanager-linux-x86_64-${DRIVER_VERSION}-archive.tar.xz"
WORK=/var/tmp/nvidia-recipe
LOG=/var/log/nvidia-gpu-recipe.log

exec > >(tee -a "$LOG") 2>&1
echo "=== $(date -u +%FT%TZ) nvidia-gpu-recipe on $(hostname -f), kernel $(uname -r)"

if ! grep -qx 0x10de /sys/bus/pci/devices/*/vendor 2>/dev/null; then
  echo "No NVIDIA PCI device; nothing to do."
  exit 0
fi

fetch() { curl -fsSL --retry 5 --retry-delay 10 -o "$2" "$1"; }
mkdir -p "$WORK"

# --- driver -----------------------------------------------------------------------------------
if command -v nvidia-smi >/dev/null && nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | grep -qx "$DRIVER_VERSION"; then
  echo "Driver $DRIVER_VERSION already installed."
else
  test -d "/lib/modules/$(uname -r)/build" || { echo "kernel-devel for $(uname -r) missing"; exit 1; }
  printf 'blacklist nouveau\noptions nouveau modeset=0\n' > /etc/modprobe.d/blacklist-nouveau.conf
  if lsmod | grep -q '^nouveau'; then modprobe -r nouveau; fi
  fetch "$DRIVER_URL" "$WORK/driver.run"
  sh "$WORK/driver.run" --silent --kernel-module-type=open --no-opengl-files \
     --no-x-check --no-nouveau-check --log-file-name="$WORK/nvidia-installer.log"
  dracut -f   # keep nouveau out of the initramfs for the reboot
fi

# --- persistence daemon -----------------------------------------------------------------------
cat > /etc/systemd/system/nvidia-persistenced.service <<'EOF'
[Unit]
Description=NVIDIA Persistence Daemon
Wants=syslog.target

[Service]
Type=forking
ExecStart=/usr/bin/nvidia-persistenced --verbose
ExecStopPost=/bin/rm -rf /var/run/nvidia-persistenced

[Install]
WantedBy=multi-user.target
EOF

# --- Fabric Manager (tarball's own installer copies libs to a Debian path; do it for RHEL) -----
fetch "$FM_URL" "$WORK/fm.tar.xz"
rm -rf "$WORK/fm" && mkdir -p "$WORK/fm"
tar -xJf "$WORK/fm.tar.xz" -C "$WORK/fm" --strip-components=1
install -m 0755 "$WORK"/fm/bin/{nv-fabricmanager,nvswitch-audit,nvidia-fabricmanager-start.sh} /usr/bin/
cp -P "$WORK"/fm/lib/libnvfm.so* /usr/lib64/ && ldconfig
mkdir -p /usr/share/nvidia/nvswitch /usr/share/doc/nvidia-fabricmanager
cp "$WORK"/fm/share/nvidia/nvswitch/* "$WORK"/fm/etc/*.cfg /usr/share/nvidia/nvswitch/
cp "$WORK"/fm/{LICENSE,third-party-notices.txt} /usr/share/doc/nvidia-fabricmanager/
install -m 0644 "$WORK/fm/systemd/nvidia-fabricmanager.service" /etc/systemd/system/

systemctl daemon-reload
systemctl enable --now nvidia-persistenced
systemctl enable --now nvidia-fabricmanager

# --- cgroup v1 for YARN GPU isolation (next boot) -----------------------------------------------
grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=0"

nvidia-smi -L
systemctl is-active nvidia-fabricmanager
echo "=== done; reboot required for cgroup v1 (now: $(stat -fc %T /sys/fs/cgroup))"
rm -rf "$WORK"
