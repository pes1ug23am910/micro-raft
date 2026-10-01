# Checking read/write histories

`scripts/check_history.py` checks finite histories of independent string registers. Each operation is a PUT, DELETE, or GET on one key; `null` represents an absent key, while an empty string is a stored value. The initial value of every observed key must be supplied explicitly. A fresh empty cluster can use `null`; an existing data directory cannot be assumed empty.

The checker searches for a sequential order compatible with the observed values and real-time precedence. A response strictly earlier than another invocation must precede it. Equal timestamps do not establish an order. Overlapping operations may appear in either order. Keys are checked separately because these operations do not provide transactions across keys.

This is bounded memoized backtracking, following the general history-search approach described by [Wing and Gong](https://www.cs.cmu.edu/~wing/publications/WingGong93.pdf) and [Lowe](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/). It is a testing tool, not a proof of the service for all executions.

## Results and uncertain outcomes

- **PASS / exit 0:** at least one allowed completion and sequential ordering explains the history. The result includes a witness for each checked key. Attempt counts, outcomes, logical operation counts, and successful read counts remain explicit: an empty or entirely unknown prefix can pass without establishing any successful read.
- **FAIL / exit 1:** the search finished without finding an allowed ordering. The offending key is identified. `--reduce` removes redundant reads while retaining all mutations, so removing a write cannot manufacture a missing-value error.
- **INCONCLUSIVE / exit 2:** the input is unsupported or incomplete in a way that prevents checking, or the search/operation limit was exceeded. A search budget is never converted into PASS.

An acknowledged mutation must occur between its invocation and successful response. A definite rejection has no effect. A timed-out or otherwise unknown mutation may be omitted, or may occur any time after invocation, including after the client timeout. Its timeout is not a server cancellation boundary. An unanswered read can be omitted because it supplies no observed value. PASS therefore does not establish which uncertain writes actually committed.

A retry-protected mutation can carry `logical_id` and `retry_protected: true`. Attempts with the same logical identity and identical payload are folded into one mutation, from the earliest invocation to the earliest successful response, if any. Later cached success responses do not mutate the key again. Different payloads with that identity are invalid input; definite rejected attempts do not become mutations. Unprotected attempts remain separate operations even when their key and payload match.

All intervals must come from one named monotonic clock domain. Cross-host wall clocks are not accepted as interchangeable. Only reads explicitly recorded as `linearizable` can establish a checked history; the older local read mode is insufficient.

## File format

```json
{
  "schema_version": 1,
  "clock_domain": "one-observer-monotonic-clock",
  "initial": {"key": null},
  "operations": [
    {"id":"write-1","kind":"put","key":"key","value":"value","invoke_ns":10,"complete_ns":20,"outcome":"ok"},
    {"id":"read-1","kind":"get","key":"key","value":"value","invoke_ns":30,"complete_ns":40,"outcome":"ok","read_mode":"linearizable"}
  ]
}
```

Use `outcome: "unknown"` and a recorded timeout or `null` completion for an uncertain operation. `outcome: "rejected"` requires a definite no-effect response. Operation IDs identify individual attempts and must be unique. Do not reuse an attempt ID to conceal a retry.

```sh
python3 -B scripts/check_history.py history.json --budget 100000 --reduce
```

The default limit is 256 logical operations and 100,000 explored states across keys. Input size is bounded at 64 MiB. The optional reduction shares one additional budget of the same size across all its searches after a FAIL. It reports its own state count and whether reduction finished; exhaustion leaves the original FAIL and a still-valid, possibly larger counterexample.

## Recording live requests

`record_history.py` uses the same bounded HTTP worker as the container client. A workload supplies explicit node origins, known initial values and batches of concurrent operations. Batches run sequentially; up to 16 attempts in a batch run concurrently. There are no automatic redirects or retries. A request retains its chosen node even if leadership changes.

```json
{
  "schema_version": 1,
  "endpoints": {"node1":"http://127.0.0.1:8100"},
  "initial": {"example":null},
  "batches": [
    [{"id":"put-1","node":"node1","kind":"put","key":"example","value":"hello"}],
    [{"id":"get-1","node":"node1","kind":"get","key":"example"}]
  ]
}
```

```sh
python3 -B scripts/record_history.py record workload.json new-journal.jsonl --timeout 3
python3 -B scripts/record_history.py check new-journal.jsonl
```

GET requests ask for `?consistency=linearizable`; a successful response must declare `X-Raft-Read-Mode: linearizable`. A server that ignores the query and returns a local read causes INCONCLUSIVE. Node status or a leader label alone does not upgrade a local read.

PUT/DELETE attempts may include positive integer `session_id` and `sequence` fields after registration. The recorder derives a key-scoped logical identity from them and uses the protected endpoint. A successful protected response must echo the requested session and sequence. Two explicit retry attempts have different `id` values and the same session/key/sequence/payload.

The journal is created exclusively and never overwrites an older run. Invocations are synced before requests start, and raw responses are synced before the recorder returns. Concurrent append writes are serialized. Every timestamp comes from the same observer process. A valid invocation without a response becomes unknown when replayed. Replay validates the full header and every attempt, including protected session fields and the endpoint map; it verifies each raw observation's schema, node/run/cycle/attempt identities, clock domain, and duration against its timestamps. A missing field, inconsistent provenance, torn JSON line, mixed clocks, duplicate ID or orphan completion causes INCONCLUSIVE. JSONL records are separated by physical newlines; Unicode line separators inside strings are ordinary content. Checking a journal checks the recorded prefix, not an assertion that an intended workload finished. Disk errors invalidate the recording. File sync is required on both supported platforms; journal directory sync is also required on POSIX. These are syscall-level guarantees, not evidence of physical power-loss survival.

The recorder limits both a workload and a replayed journal to 256 attempts, including rejected and pending attempts. Workload and journal inputs are limited to 64 MiB. The three-second default is a client observation deadline; unknown operations remain in the model. Record workload configuration, source/image identity, fault confirmations and raw journal alongside a live result. Hand-made histories test the checker itself; they are separate from live service validation and deliberately planted read-protocol defects.

Run the focused checker/recorder regressions with:

```sh
python3 -B -m unittest discover -s scripts -p 'test_*history.py' -v
```
