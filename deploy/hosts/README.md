# Three-VM experiments

`host_experiment.py` prepares a small prebuilt bundle and runs the same acknowledged-state oracle used by the Compose harness across three separately identified Linux VMs. It does not provision a cloud account or prove three separate physical machines. The controller runs on VM1; each VM runs one replica. The observer remains alive when a replica process is killed or paused.

Prerequisites are Python 3.10+, systemd, iproute2 with IFB/netem/flower support, and passwordless scoped administrative access on disposable experiment VMs. SSH uses existing verified host keys and `BatchMode`; it never disables host verification. VM1 needs access to the other two VMs through a private interface. An operator may use an ephemeral experiment SSH identity or a forwarded SSH agent on these trusted VMs. Provider API credentials are unnecessary on the VMs. Keep public Raft/HTTP ports closed; the binary binds only the configured private addresses. The plain Raft protocol is still a trusted-network protocol.

Copy `topology.example.json` to a private working file. Fill in real VM identities, three distinct RFC1918 addresses, a fresh `mr-...` run name, the exact prebuilt binary SHA256, installed bundle paths, and an absolute Unix expiration time at most two hours away. Obtain the binary and source/build manifest before the paid experiment window. VM1 is the sole `local` node. The other nodes use their private addresses for SSH. Record the provider/hypervisor inventory, source revision and dirty-source manifest with the topology; changing a label does not establish machine identity.

```sh
python3 -B scripts/host_experiment.py prepare \
  --topology /private/topology.json --binary /build/kv-node \
  --out /private/fresh-bundle
```

The bundle contains the binary, existing HTTP client, shared oracle, controller, agent, three node configurations and a SHA256 manifest. Install this same bundle at the topology's unique path on each VM, preserving its directory layout. Verify the hashes after transfer. Each `config_path` points to that member's generated JSON; each `agent_path` points to `deploy/hosts/host_agent.py`. No runtime compiler or container image download is needed.

Run from VM1, with a fresh output directory outside the bundle:

```sh
python3 -B /opt/micro-raft/RUN/scripts/host_experiment.py run \
  --topology /opt/micro-raft/RUN/topology.json \
  --out /var/tmp/RUN-evidence --case healthy --operations 8
```

Cases are `healthy`, `delay`, `jitter`, `loss`, `partition`, `kill`, `pause`, and `restart`; `--case all` plans each in that order. Each failed or inconclusive attempt remains in the result and stops later cases, whose unattempted count is explicit. Use fresh run names/data for independent cases. `restart` sends SIGTERM, requires a verified successful exit and holds the old process stopped until a replacement leader proves service. `kill` sends SIGKILL. No fixed repetition count establishes reliability.

Delay adds 25 ms on each selected receiving path; jitter uses 25 ± 10 ms; random loss is 10%. Partition uses 100% loss in both directions between the selected member and its peers. The agent redirects only private peer Raft TCP and dedicated UDP probe traffic from ingress into its own IFB/netem device. HTTP observations and SSH are excluded. This placement also avoids treating sender-side TCP queueing as a realistic receiver impairment. See the upstream [netem description](https://kernel.googlesource.com/pub/scm/network/iproute2/iproute2-next/+/refs/heads/main/man/man8/tc-netem.8).

The harness retains actual qdisc/filter readbacks, packet counters and independent UDP round-trip/loss samples before and during shaping. A command's success alone does not confirm a fault. Small random samples may fail to demonstrate the requested loss or jitter; that is inconclusive. Probe round trips use each source VM's monotonic clock; only the VM1 observer clock orders operation, command-issue, fault-confirmation and service-recovery events. Remote sample timestamps are diagnostics and are not merged into a cross-host timeline.

The HTTP client is reused unchanged. Only a complete validated success response enters the synced ACK ledger. Unknown outcomes remain unknown. Read-back requires a unique observed leader and at least one same-term follower that names it, within the verified eligible component, before and after local GETs. A third same-term campaigning member does not erase that majority; a second observed leader or a newer observed term disqualifies it. Applied-index fences, reopened-member catch-up and a post-heal write remain required. This is an acknowledged-state check after stabilization, not a linearizability test or ReadIndex evidence. The workload has one outstanding write at a time and includes read-back between writes; its observed rates are diagnostic, not a saturation throughput benchmark.

Per-node samples include raw process CPU ticks, RSS pages, process I/O, host interface counters, disk counters and stored-data bytes, with tick frequency and page size. Samples are attempted once per second; process identity changes are recorded and excluded from process statistics. Host disk/network totals include the whole VM. Retain sampling gaps, unknown requests and failures when computing rates or distributions.

Guest UTC clocks must agree with the observer within a five-second tolerance for lease admission; operation ordering still uses only the observer monotonic clock. Every mutation checks a run ownership nonce, immutable config identity, service description and exact network-rule selector. Before enabling an IFB, the agent records its interface index, MAC, type, boot ID and network namespace, explicitly sets its ownership alias, and verifies the readback. Cleanup rechecks that identity and the expected qdisc before deletion. A missing label or changed identity causes failure and retains the uncertain device for reconciliation; a successful creation command alone is insufficient. A default-route interface is refused. Existing unrelated qdiscs/filters are left intact. Cleanup removes only the agent's own IFB/filter resources and stops its services; data and logs remain under `/var/lib/micro-raft-experiments/RUN/nodeN` for collection. Missing evidence or surviving owned processes/network rules makes the result fail. Accept PASS only together with exit zero and a complete, readable result; storage errors invalidate publication. Log/metadata collection is capped at 16 MiB per file and 32 MiB total including hash-verified node data; exceeding those bounds fails collection and requires a separate export before VM deletion. Output includes raw agent calls and per-node evidence, even after an earlier assertion failed.

The controller reserves the last two minutes of its workload lease for collection. Node services have `RuntimeMaxSec`. Restart retires the verified stopped transient unit, waits for it to unload and recreates it with the remaining absolute lease; it does not depend on runtime mutation of that systemd property. A separate in-guest watchdog restores network rules and stops processes if the controller disappears. These bounds do **not** stop cloud billing. An independent external lifecycle controller must journal the exact created resource IDs, enforce the approved cumulative budget, export evidence, destroy only those VMs and any dedicated empty network, and verify provider inventory. Do not infer deletion from SSH loss, guest shutdown, a successful destroy request or a PASS here. If cleanup cannot be verified, preserve that failure and reconcile the owned resources externally.

```sh
python3 -B -m unittest discover -s scripts -p test_host_experiment.py -v
```

The local fixtures cover command construction, ownership boundaries and outcome classification. Actual VM operation, network effects, systemd lifecycle and provider teardown need separately recorded runtime evidence.

## Recorded local VM run

On 2026-10-01 IST, three Ubuntu 24.04 KVM guests (one vCPU and 512 MiB each) on one WSL/Windows host exercised all eight cases. Their private bridge had no uplink; Raft/probe shaping excluded HTTP and SSH. The runtime was sealed separately from the harness, before the later snapshot changes: source archive SHA256 `2ff4b9530de538369bbed77c88e8bfc8476bae8a3c24ab0c2d29f6fcec485f76`, Ubuntu binary SHA256 `a8a3bdd34c95a3093e4233f65de26069017086ed3ba579b1f5ff1de35bb29eef`.

There were 11 independent invocations: eight passed, two failed on host-tooling defects and one was inconclusive. The initial delay run exposed a silently missing IFB ownership alias; cleanup correctly refused the unlabelled device. The initial kill run exposed unsupported runtime mutation of `RuntimeMaxSec`. Both failures were retained, corrected and rerun in fresh namespaces. The first loss run lost the old all-member qualification when one follower entered `pre_candidate`, although the leader and another follower stayed aligned in the same term. Its two offered ACKs and six unoffered writes remain in the record. A separately tested majority qualifier and a fresh loss run passed with the original deadlines. The initial inconclusive result was not reclassified.

Each of healthy, delay, jitter, random loss, partition, SIGKILL, pause and held graceful restart therefore has one passing run. The full record contains 60 validated write ACKs, including probes and post-heal writes, and six planned workload writes that were never offered. Actual shaping readbacks, counters, UDP probes, resource samples, operation brackets and failed outcomes were retained. After collection, all three VM processes and the owned bridge/taps were verified absent; disks and logs were retained privately. No cloud resources were created.

This is small-sample functional evidence from separate guest kernels on one physical machine. Random loss and election seeds were not pinned. Shared-host memory pressure and variable election terms prevent a throughput, availability or reliable failover-latency claim. Later runtime revisions need their own evidence.
