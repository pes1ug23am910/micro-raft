# Shard placement comparison - 2026-10-01

The equal-weight Ketama policy moved fewer shards than hash-mod-N in all four
declared group changes. It did not consistently improve balance. For example,
adding a fourth group moved 26 of 64 shards with 160-point Ketama, compared
with 48 using mod-N, while its maximum key load was about 1.62 times the mean.
These are computed placements, not measurements of live migration.

## Results

Each range covers three populations of 100,000 keys; group labels and the
64-shard placement are fixed across those populations. Max/mean includes
every destination group, including any with zero assigned load.

| Group change | Policy | Moved shards / 64 | Moved keys (%) | Max/mean key load after |
|---|---|---:|---:|---:|
| 2 to 3 groups | Mod-N | 44 | 68.54-69.03 | 1.17-1.17 |
| 2 to 3 groups | Ketama, 160 points | 17 | 26.22-26.60 | 1.17-1.18 |
| 3 to 4 groups | Mod-N | 48 | 74.96-75.28 | 1.31-1.32 |
| 3 to 4 groups | Ketama, 160 points | 26 | 40.33-40.67 | 1.61-1.63 |
| 7 to 8 groups | Mod-N | 53 | 82.85-83.06 | 1.38-1.39 |
| 7 to 8 groups | Ketama, 160 points | 12 | 18.59-18.99 | 1.50-1.52 |
| 8 to 7; remove group-02 | Mod-N | 55 | 85.80-85.88 | 1.30-1.32 |
| 8 to 7; remove group-02 | Ketama, 160 points | 4 | 6.22-6.28 | 1.31-1.33 |

The 3 to 4 case illustrates sensitivity to the continuum size:

| Points per group | Moved shards / 64 | Max/mean key load after | Max/mean synthetic demand after |
|---:|---:|---:|---:|
| 40 | 17 | 1.31-1.33 | 1.24-1.49 |
| 160 | 26 | 1.61-1.63 | 1.34-1.95 |
| 640 | 15 | 1.38-1.39 | 1.15-1.50 |

More continuum points do not create more shards. With only 64 indivisible
units, uneven placement and a popular key can dominate group load. The full
[JSON result](shard-placement-2026-10-01.json) includes every route before and
after, moved shard IDs, per-shard populations, moved logical bytes and relative
demand, per-group loads, coefficients of variation, and continuum collisions.

## Protocol

- Seeds: `20261001`, `20261002`, `20261003`; key `placement/{seed}/{rank}`, with
  rank 0-99,999. The population witness hashes every key, shard and value size.
- A key maps to a shard using SHA-256, its first eight bytes interpreted as
  big-endian, modulo 64. This matches the service's key-to-shard rule.
- The placement input is `shard-{id}`. Both policies use the first four MD5
  bytes interpreted as little-endian. Mod-N indexes the sorted group list.
- Ketama uses equal group weights, labels `group-01` through `group-08`, and
  40, 160 or 640 points per group. Each MD5 of `group-label-replica` supplies
  four little-endian points. Lookup chooses the first point at or above the
  shard hash, wrapping at the end. A point collision chooses the first group
  lexicographically. This convention follows the [original Ketama design](https://github.com/RJ/ketama);
  it is a bounded equal-weight implementation, not a general compatibility claim.
- Logical byte weight is UTF-8 key length plus a deterministic synthetic
  value size of 16-1,024 bytes. It excludes retry records, Raft replication,
  snapshot encoding and storage/transport overhead.
- Relative demand weights each key by `(rank + 1)^-1.1`. No requests are sent;
  these weights expose sensitivity to synthetic key popularity.

The script checks conservation of keys, bytes and demand. For Ketama, it also
requires additions to move shards only to newly added groups, and removals
to move only shards that the removed group previously owned. Unit checks use
an independent exhaustive circular-distance oracle for the binary search,
known MD5 vectors, handmade load totals and preservation of existing output.

## Reproduce

```sh
python -B -m unittest discover -s scripts -p test_placement_experiment.py -v
python -B scripts/placement_experiment.py --output .local/placement-fresh
```

The output directory must be new. `run.json` records the interpreter, host,
script hash, invocation, verdict and result hash. `placement.json` contains
all 48 cases. A failure retains a failed/incomplete manifest and cannot pass.

The recorded run used Windows Python 3.14.4; Linux Python 3.12.3
repeated all 48 cases. Seven unit checks passed on each platform. All integer
routes, counts, byte totals and workload witnesses agreed exactly; floating
demand metrics were compared with relative/absolute tolerance `1e-13`.

Experiment script SHA-256:
`499fc6e003b8cf1ca10debd128a5f679967f0170b334f334d0559e37b1cd540b`.
Published result SHA-256:
`839fde8a6b5f80bd39aa64a27e432dffa6dee21b3ac161abab32aee69c4adbaa`.

These runs evaluate the same fixed rings; they are not six independent ring
samples or a latency benchmark. The script does not update controller routes.
A chosen route must still be committed and executed through source fencing,
state transfer, destination activation and safe cleanup. Live handoff recovery
tests provide separate evidence for those mechanisms.
