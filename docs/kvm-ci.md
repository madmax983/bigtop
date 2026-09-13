# KVM CI lane (Tier 1)

BigTop's CI has two tiers:

| Tier | Workflow | Runners | What it proves |
|------|----------|---------|----------------|
| 0 | `.github/workflows/ci.yml` | GitHub-hosted `ubuntu-latest` | fmt, strict clippy, all unit + process-runtime e2e tests |
| 1 | `.github/workflows/kvm.yml` | **self-hosted, KVM-capable** (`[self-hosted, linux, kvm]`) | a **real** Firecracker microVM boots, runs guest code, snapshots live, and resumes from the snapshot |

Tier 0 never touches a hypervisor. Tier 1 is the only place the claim
"BigTop boots real microVMs" gets verified.

## Why not GitHub-hosted runners?

Firecracker's own getting-started guide requires Linux with KVM and
read/write access to `/dev/kvm`
([source](https://github.com/firecracker-microvm/firecracker/blob/fea3897ccfab0387ce5cd4fa2dd49d869729d612/docs/getting-started.md)).
Ordinary GitHub-hosted runners do not expose `/dev/kvm`
([GitHub Next's sandbox design notes](https://github.com/githubnext/gh-aw-firewall/blob/HEAD/docs/sandbox-design.md),
[Actuated's KVM-in-Actions write-up](https://actuated.com/blog/kvm-in-github-actions)).
So Tier 1 is pinned to `runs-on: [self-hosted, linux, kvm]` and starts
with a **fail-fast preflight**: if `/dev/kvm` is absent or inaccessible
the job errors immediately instead of silently skipping the tests.
(The tests themselves also skip cleanly with a printed reason when the
prerequisites are missing, so a dev laptop never goes red.)

## Runner options

Pick whichever you control. Mark, this choice is yours — nothing below
provisions anything; it only runs when you register a runner with the
`self-hosted, linux, kvm` labels.

### Option A — self-hosted runner (bare metal or nested VM)

Any Linux x86_64 machine with KVM: a spare box, or a VM with nested
virtualization enabled. Install the GitHub Actions runner, register it
with labels `self-hosted, linux, kvm`, and make sure:

- `/dev/kvm` exists and the runner user can read/write it
  (`usermod -aG kvm <runner-user>`, then re-login),
- `curl`, `mke2fs` (e2fsprogs), `sha256sum`, and `sudo` are installed,
- a Rust toolchain is on `PATH` (`rustup` is fine).

### Option B — ephemeral cloud VM with nested virtualization

Spin up a KVM-capable VM per run (or per night), register it as an
ephemeral runner, tear it down afterwards. Verified path: Google Cloud
Compute Engine — nested virtualization is supported on N1/N2 machine
series via `--enable-nested-virtualization`
([official docs](https://docs.cloud.google.com/compute/docs/instances/nested-virtualization/enabling)).
AWS bare-metal instances (`.metal`) are the equivalent on that side.
Teardown is your responsibility; the workflow itself provisions nothing.

### Option C — managed KVM-capable vendor: Namespace

[Namespace](https://namespace.so) documents nested virtualization as an
opt-in instance feature: setting `nested_virtualization` exposes
`/dev/kvm` inside the instance, and their changelog names exactly this
use case — *"run Firecracker, QEMU, or Vagrant boxes inside your CI
instance for integration tests that require a real VM boundary"*
([changelog](https://namespace.so/blog/changelog-018)).
Their Android-emulator docs likewise confirm KVM-backed runners on
`linux/amd64` with no extra setup
([docs](https://namespace.so/docs/integrations/android-emulators)).

### Honest footnotes

- **GitHub's own larger runners** (paid tier) do expose KVM for
  hardware-accelerated Android virtualization
  ([changelog, 2023-02-23](https://github.blog/changelog/2023-02-23-hardware-accelerated-android-virtualization-now-available-for-github-actions-larger-linux-runners/)).
  That is a verified fact, but this lane is deliberately built on the
  self-hosted label set above, not on GitHub's paid tier.
- **Ubicloud**: one third-party source claims their runners lack nested
  virtualization. That is not vendor documentation, so treat Ubicloud as
  **unverified** — ask them before relying on it.

## What the lane does

1. **Preflight** — fail fast if `/dev/kvm` is missing/inaccessible.
2. **Install Firecracker** — downloads `firecracker-v1.17.0-x86_64.tgz`
   from the official release and verifies SHA-256 before installing.
3. **Provision guest artifacts** — downloads the official Firecracker
   quickstart kernel (SHA-256 verified) and builds a minimal ext4
   rootfs via `scripts/kvm/build-rootfs.sh` (pinned static BusyBox +
   `scripts/kvm/guest-init.sh` as `/sbin/init`). No guest binaries are
   committed to the repo.
4. **Run the tests** — `cargo test -p bigtop-agent --test kvm_boot`,
   which boots a real microVM, asserts guest output on the serial
   console, snapshots the live VM, kills it, restores under a new task
   id, and asserts execution resumes.

## Pinned artifacts

| Artifact | Source | SHA-256 (measured 2026-09-13) |
|----------|--------|-------------------------------|
| `firecracker-v1.17.0-x86_64.tgz` | [firecracker-microvm/firecracker releases](https://github.com/firecracker-microvm/firecracker/releases/tag/v1.17.0) | `06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558` |
| `vmlinux.bin` (quickstart kernel) | `https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/x86_64/kernels/vmlinux.bin` | `264c461809cec3961b162d241b66a4b004f194fbaa44b1570f3f61c316d8ea69` |
| `busybox` 1.35.0 x86_64 musl static | `https://busybox.net/downloads/binaries/1.35.0-x86_64-linux-musl/busybox` | `6e123e7f3202a8c1e9b1f94d8941580a25135382b99e8d3e34fb858bba311348` |

To re-pin: download from the official source, `sha256sum` the file,
update the hash in `.github/workflows/kvm.yml` (firecracker, kernel) or
`scripts/kvm/build-rootfs.sh` (busybox), and note the date you measured
it in this table.

## Running the tests locally

On any KVM-capable Linux box:

```bash
# one-time provisioning
bash scripts/kvm/build-rootfs.sh target/kvm-guest
curl -fsSL --http1.1 -o target/kvm-guest/vmlinux.bin \
  https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/x86_64/kernels/vmlinux.bin

# run (skips cleanly without /dev/kvm)
BIGTOP_KVM_KERNEL=$PWD/target/kvm-guest/vmlinux.bin \
BIGTOP_KVM_ROOTFS=$PWD/target/kvm-guest/rootfs.ext4 \
cargo test -p bigtop-agent --test kvm_boot -- --nocapture
```

Without `/dev/kvm` (or without the env vars) the tests print `SKIP: …`
and pass. That is the designed behavior, not a gap: the *workflow's*
preflight is what turns a missing prerequisite into a failure.
