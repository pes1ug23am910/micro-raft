# Live histories during container faults

`scripts/live_history_experiment.py` combines the three-node Compose fault runner with the checked-read [history recorder and checker](read-history.md). It uses the existing bounded HTTP client. It runs one fault in one fresh project namespace; run each of `kill`, `pause`, `partition`, and `restart` separately so an earlier failure does not hide unattempted cases.

The node and client images must already exist locally. The runner inspects their immutable IDs, uses those IDs with pulling and building disabled, and verifies every observed container's image. Supply the exact source directory used to build both images through `--sealed-source`. Recorder/checker files used outside the observer must match that directory, and the client copied into the observer must match the client image. Preserve the source hashes, image IDs, harness hashes, command, seed, and raw output together. A new source revision requires new evidence.

For example, on a Linux host with Python 3 and Docker Compose available:

```sh
SEALED=/absolute/path/to/source-snapshot
docker build -f "$SEALED/deploy/docker/Dockerfile" -t micro-raft:history "$SEALED"
docker build -t micro-raft-client:history "$SEALED/deploy/docker/client"
python3 -B scripts/live_history_experiment.py \
  --compose "$SEALED/deploy/docker/compose.3.yml" \
  --sealed-source "$SEALED" \
  --project mr-history-kill-001 --out ./evidence/history-kill-001 \
  --fault kill --seed 2026100101 \
  --node-image micro-raft:history --client-image micro-raft-client:history
```

Choose a fresh project and empty output directory for every invocation. The script needs Docker access and is intended for a disposable local test environment. It can also run in `deploy/docker/runner/Dockerfile`'s image with the harness mounted at `/work`, the sealed source at `/sealed`, an empty writable output directory at `/evidence`, and the engine socket mounted at `/var/run/docker.sock`; override the image entrypoint with `python3 /work/scripts/live_history_experiment.py` and use those container paths. Docker socket access grants control of that engine.

## What a passing run establishes

There are two separate oracles. The inherited fault runner records acknowledged writes, holds the old leader unavailable until a replacement serves the surviving majority, checks the acknowledged values through topology-qualified local reads, heals the fault, and verifies catch-up and another write. Its local reads retain their stated scope.

The history worker records actual `?consistency=linearizable` GETs using one persistent observer process and monotonic clock. It checks two initially absent keys in fresh volumes:

- One successful baseline PUT and checked GET before the fault.
- Nine seeded concurrent batches, each with one PUT or DELETE and a checked GET against each of the three nodes: 36 attempts total.
- One successful checked GET during the confirmed held fault and one after healing.

All 36 background attempts must be invoked and finish their client observations within the confirmed fault window. The controller waits for the finite background workload before healing. The four mandatory operations bring the total to exactly 40; missing or duplicated attempt IDs, unfinished batches, invalid observation provenance, missing fault markers, or a missing successful mandatory read prevent acceptance. A returned local read never satisfies a checked-read requirement.

Fault issue, confirmation, heal issue, and heal confirmation are recorded by the same observer that timestamps requests. Runner timings use a separate domain and must not be subtracted from observer timestamps. The seed determines the workload, not election timing, operating-system scheduling, or packet delivery.

Each request has a two-second observation deadline. There are no automatic redirects or retries. Timeouts and malformed responses remain unknown; they are never discarded or counted as acknowledged writes. An unknown mutation can still have committed after its client timeout. The bounded checker must find an allowed sequential ordering; search exhaustion is INCONCLUSIVE. Successful acceptance also requires the fault runner's oracle and complete owned-resource cleanup. PASS does not prove the service correct for every execution, identify which unknown mutations committed, or establish exactly-once session behavior.

## Fault and evidence boundaries

`kill` sends SIGKILL. `pause` suspends the leader process. `restart` requests a graceful stop, verifies exit zero, and holds the process stopped until a replacement has served the majority. `partition` disconnects the leader's entire Compose network, including its HTTP endpoint; this case does not test an isolated old leader that remains reachable by HTTP. These are three containers on one Docker host, not three independent machines or a WAN experiment.

The output retains the exact workload, raw invocation/completion journal, observer metadata, fault markers, checker witness or counterexample, mandatory-read acceptance, acknowledged-write ledger, image inspections, node logs, command outputs, phase events, and cleanup inventory. Failed and incomplete runs should remain alongside successful ones. The checker can explain an empty or entirely unknown prefix; the experiment's stricter acceptance rules prevent that prefix from becoming a successful live fault run.

Cleanup removes only containers and networks owned by the selected Compose project. The three node volumes and observer ledger volume are retained intentionally and named in `cleanup.json`; retain them for diagnosis or remove those exact volumes separately after review. Collection and cleanup failures invalidate the final experiment verdict. Request and subprocess deadlines do not impose a hard deadline on a blocked filesystem sync.

Run the adapter/worker fixtures with:

```sh
python3 -B -m unittest discover -s scripts -p test_live_history_experiment.py -v
```
