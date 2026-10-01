# Client sessions and read consistency

Writes are completed only after their committed log entries have been applied.
A lost response or `outcome_unknown` does not establish whether a mutation took
effect. Legacy `PUT /kv/{key}` and `DELETE /kv/{key}` assign a new log entry to
each attempt. The session API provides a bounded identity for safe retries.

## Retained sessions

Register with `POST /sessions` and a raw UTF-8 nonce, at most 128 bytes without
control characters. Registration is replicated; the first registration's log
index becomes the session ID. Repeating the nonce returns the original ID,
including after close. A close never reopens the session.

For a session and key, start at sequence 1 and advance by one after each
confirmed operation:

```sh
curl -X POST --data-binary 'client-registration-1' http://127.0.0.1:8101/sessions
curl -X PUT --data-binary 'value' 'http://127.0.0.1:8101/sessions/2/kv/key?sequence=1'
curl -X DELETE 'http://127.0.0.1:8101/sessions/2/kv/key?sequence=2'
curl -X DELETE http://127.0.0.1:8101/sessions/2
```

The example ID `2` is illustrative: use the actual registration response.
A protected success includes `ok`, `session_id`, `sequence` and the original
operation's `index`. An identical retry of the latest sequence returns that
cached response without mutating the key again, even if a later independent
write changed its value. The operation kind and exact payload must match;
a changed payload is rejected. Deletion retains its retry record.

Keep one outstanding logical operation per session/key. Sequence numbers are
independent across keys. A gap, stale sequence, or changed payload returns 409;
an unknown session returns 404; a closed session returns 410. Capacity errors
return 507. Malformed requests return 400. These application rejections are
replicated outcomes and do not apply the rejected mutation.

Retention lasts until database reset, with no time expiry, eviction or reuse.
Limits are 1,024 sessions, 256 keys per session, 8,192 session/key records in
total, and 16 MiB of retained canonical request payloads. Closed sessions still
consume capacity. Keys are at most 1 KiB and values at most 64 KiB. Only the
latest response for each session/key is retained: an older sequence cannot be
replayed after advancing. A domain-separated SHA-256 fingerprint and the exact
canonical bytes are compared, so correctness does not depend solely on a hash.

Session IDs currently identify one database/group. They are not global client
identities across unrelated clusters. This contract does not make arbitrary
external side effects or legacy writes exactly once. A client must preserve
its registration nonce, session ID, key and sequence across its own restart.

## Checked reads

`GET /kv/{key}` and `?consistency=local` read the local application state on any
node. `X-Raft-Read-Mode: local`, `X-Raft-Role` and `X-Raft-Last-Applied` describe
that observation; a leader label alone does not establish freshness.

`GET /kv/{key}?consistency=linearizable` enters the single-writer driver. A
leader must first commit an entry in its current term (normally its election
no-op). It captures the commit index, sends a new term-scoped read context,
and waits for distinct eligible majority replies for exactly that context.
Old contexts, old terms, duplicate voters and ineligible peers cannot supply
this confirmation. Read probes carry no log entry and do not replace the
independent CheckQuorum contact-round protocol.

After confirmation the driver waits until the application has applied through
the captured index. The value and actual applied watermark are read under the
same lock. The core's scheduled apply index is not used as evidence that the
application has executed those effects. A leadership/term change, shutdown,
closed channel or deadline rejects the read instead of falling back to local
state. The two-second HTTP budget includes queueing; the driver's independent
two-second waiter budget bounds cleanup. Neither is a deadline on a blocked
synchronous filesystem call. There are at most 64 queued and 128 driver-waiting
reads, with checked request/context counters that never wrap.

Successful checked reads return 200 for a value or 404 for absence. Both carry:

| Header | Meaning |
|---|---|
| `X-Raft-Read-Mode: linearizable` | The checked path completed |
| `X-Raft-Role: leader` | Role at completion |
| `X-Raft-Read-Index` | Commit index captured before the fresh quorum round |
| `X-Raft-Last-Applied` | Actual application watermark, at least the read index |
| `X-Raft-Term` | Confirmed leader term |
| `X-Raft-Read-Context` | Nonzero term-scoped read context |

Failures return 503 with an explicit reason such as `not_leader`,
`read_not_ready`, `leadership_lost`, `read_timeout` or `read_capacity`.
Leader hints are advisory; clients choose explicit retry destinations. Unknown
consistency values return 400. There is no follower forwarding or time-based
read lease. The protocol assumes trusted crash-fault peers; it does not defend
against forged sender identities or Byzantine messages.

Use the [history recorder and checker](read-history.md) to test finite
single-key histories, including unknown writes and retry identities. A passing
finite history is scoped evidence, not a proof for all executions. The test
suite separately checks exact quorum contexts and holds application behind a
confirmed prefix; test-only faulty branches demonstrate why removing either
check can expose an unsafe result.
