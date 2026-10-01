# Proposal batching measurement — 2026-10-01

Six fresh, three-process clusters compared single-request WAL appends with
bounded proposal batches on one Windows/NTFS host. All six runs passed: 1,584
writes received valid acknowledgements and passed readback, with no write
errors. The observed reduction in WAL append syncs did not produce a meaningful
throughput or latency improvement in this workload.

| Per-run measurement | Single-request median | Batched median |
|---|---:|---:|
| Leader WAL append syncs for 264 writes | 264 | 48 |
| Sum of replica WAL append syncs | 312 | 96 |
| Busy-phase acknowledgements/s | 255.61 | 256.33 |
| Busy-phase p95 latency | 129.58 ms | 129.77 ms |

Each run contained eight sparse sequential writes followed by 256 writes with
32 clients and 128 bytes of value padding. The paired order was AB/BA/AB:
single then batch, batch then single, and single then batch. Single mode used
one record and zero collection delay; batch mode used 16 records and 2 ms.
Automatic snapshots were disabled in both modes. The same release executable,
replication factor and acknowledgement durability applied throughout.

Leader append syncs were 264 in every single-request run and 40–51 in batched
runs. Counters describe actual WAL append-path `sync_all` calls. They exclude
hard-state writes, directory barriers, conflict rewrites, snapshots and shutdown
flushes; they do not measure physical device writes.

The source archive SHA-256 was
`7a9f04fc10754deb4319dc2b6e073fc2498980556299683ce89e16e87c256fe4`;
the Windows release executable SHA-256 was
`ba0e92b3ff79763beb113f0c366145aa0b92fb1ef040ee5afddef5079ec2074a`.
These identify the measured checkpoint, rather than implying that later source
changes were measured by the same run.

This is a small loopback experiment with three pairs. OS background activity
was not controlled, and no statistical significance or general speedup is
claimed. Cleanup hard-terminated and reaped only the experiment's owned child
processes and retained their data. This measurement does not test graceful
shutdown or physical power loss.

See [the batching protocol and reproduction command](../proposal-batching.md)
and [the machine-readable summary](proposal-batching-2026-10-01.json).
