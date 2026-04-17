# node_drain_reboot

Drain and reboot a single Kubernetes node via Ansible playbooks. First
workflow in the `maintenance` theme; intended as the template for future
maintenance workflows (rke2 upgrade, OS patch apply, firmware).

## What it does

```
resolve_target → cordon → drain → reboot → wait_ready → uncordon
```

Each arrow is a separate Temporal activity wrapping a single Ansible
playbook. Step-level retries use the shared `DEFAULT_RETRY` policy (3
attempts, 5 s initial, 2× backoff, 30 s cap). If anything past `cordon`
fails, the workflow attempts a best-effort `uncordon` before re-raising — so
a partially-failed run doesn't leave the node locked unschedulable.

Dry-run is on by default (`request.dry_run = True`) and propagates into
Ansible's `--check` mode, so every step runs without mutating cluster
state.

## Request shape

```python
from slopbox_temporal.maintenance.node_drain_reboot.models import (
    NodeDrainRebootRequest,
)

request = NodeDrainRebootRequest(
    cluster_name="prod-a",          # must exist in clusters.yaml under `kubernetes`
    node_name="prod-a-node-01",     # k8s node name
    ansible_host="10.0.0.5",        # IP or DNS Ansible will SSH to
    ansible_user="root",            # optional; default "root"
    reboot_timeout_seconds=600,     # optional; default 600
    dry_run=True,                   # safe by default
)
```

Submit to the `maintenance` task queue:

```python
from temporalio.client import Client
from slopbox_temporal.maintenance.node_drain_reboot.workflow import (
    NodeDrainRebootWorkflow,
)

client = await Client.connect("localhost:7233")
result = await client.execute_workflow(
    NodeDrainRebootWorkflow.run,
    request,
    id=f"drain-{request.node_name}-{int(time.time())}",
    task_queue="maintenance",
)
```

## Playbook contract

The workflow expects these playbooks at the root of
`$MAINTENANCE_PLAYBOOK_REPO_PATH`:

| Playbook | Role | Extra vars |
|----------|------|------------|
| `cordon_node.yml` | Mark node unschedulable | `node_name` |
| `drain_node.yml` | Evict pods, respect PDBs | `node_name` |
| `reboot_node.yml` | Reboot + wait for SSH | `node_name`, `reboot_timeout_seconds` |
| `wait_node_ready.yml` | Block until `Ready` | `node_name` |
| `uncordon_node.yml` | Mark node schedulable | `node_name` |

All playbooks target the `[target]` inventory group that the workflow
renders per-step. The inventory snippet has one host — the `node_name`
serves as an Ansible alias and `ansible_host` is the real SSH target.

## Environment variables

Consumed by the `maintenance` worker process:

| Variable | Required | Notes |
|----------|----------|-------|
| `TEMPORAL_ADDRESS` | Optional | Temporal frontend (default `localhost:7233`) |
| `TEMPORAL_NAMESPACE` | Optional | Temporal namespace (default `default`) |
| `CLUSTERS_YAML` | Optional | Path to `clusters.yaml` (default `./clusters.yaml`) |
| `MAINTENANCE_PLAYBOOK_REPO_PATH` | **Required** | On-disk path to the external playbook repo — no default |

The worker's host additionally needs an SSH key installed for `ansible_user`
on every node the workflow will be pointed at.

## Running the worker

```bash
export MAINTENANCE_PLAYBOOK_REPO_PATH=/opt/ansible/playbooks
python -m slopbox_temporal.workers.maintenance
```

## Forensics

Every playbook run has its own `private_data_dir` under `$TMPDIR`
(`/tmp/slopbox-ansible-*`). The directory contains the rendered inventory,
the full event stream, stdout, and the exit code — ansible-runner's
standard artifact layout. The path is emitted in the worker log on every
run and recorded on the `StepResult` returned from each activity.

> **Tip.** If the worker's `$TMPDIR` isn't on a persistent mount, set
> `TMPDIR` to something durable (e.g. `/var/log/slopbox/maintenance/`)
> before starting the worker so forensics survive worker restarts.

## Out of scope (today)

- Live node IP / role lookup via the k8s API — the request carries
  `ansible_host` explicitly.
- `git pull` / playbook-version-pinning — the worker's checkout is whatever
  the operator has on disk.
- Multi-node rolling maintenance — this workflow operates on a single node;
  rolling workflows will sit on top of the same `maintenance/_shared/`
  primitives.
- Approval gates / human signals between phases.

## Verification

For a dry-run smoke test against a running Temporal dev server:

```bash
export MAINTENANCE_PLAYBOOK_REPO_PATH=/opt/ansible/playbooks
python -m slopbox_temporal.workers.maintenance &
```

then submit a `NodeDrainRebootRequest(..., dry_run=True)` via a one-off
script. Every activity should log `[dry-run] running ansible-playbook <name>
(private_data_dir=/tmp/slopbox-ansible-XXXX)` and the workflow should return
`NodeDrainRebootResult.steps` with five `successful` `StepResult`s.
