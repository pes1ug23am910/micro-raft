# Separate bind addresses from reachable endpoints

## Context

A container has its own network namespace: loopback reaches that container,
while another member is reachable through a service name on the shared private
network. Legacy same-host invocations need to keep their distinct loopback
ports without making loopback a valid remote destination in explicit network
configuration.

A bind endpoint selects local interfaces. An advertised endpoint describes
where another process is intended to connect. Wildcards such as `0.0.0.0` and
`::` mean all applicable local interfaces; neither is a reachable identity.

## Decision

Configuration has two mutually exclusive forms. Legacy configuration requires
both `--raft-port` and `--http-port` and binds IPv4 loopback. Explicit
configuration requires all four of `--raft-listen`, `--raft-advertise`,
`--http-listen`, and `--http-advertise`. Mixing forms or omitting fields fails
validation instead of choosing precedence or falling back to loopback.

Listen endpoints are numeric IPv4 or bracketed IPv6 with nonzero ports;
wildcard binds are allowed. Advertisements and `id@host:port` peers retain
ASCII hostnames as well as numeric addresses. IPv6 scope IDs are unsupported.
DNS names are normalized to lowercase; an absolute name's trailing root dot is
preserved for OS lookup semantics. Numeric destinations bypass DNS. Every
resolved candidate is validated; IPv4-mapped IPv6 addresses are canonicalized
to IPv4 before comparison. Noncanonical numeric-looking host strings are
rejected rather than passed to DNS. Unspecified, multicast, and IPv4
limited-broadcast destinations are invalid, and explicit
configuration rejects loopback. Legacy loopback peers remain valid at distinct
ports.

Reject self IDs, duplicate peer IDs, duplicate configured destinations,
duplicate resolved peer sockets, and peers that alias this node's known
advertised or concrete listening sockets. A DNS alias cannot bypass a check
merely because its text differs. A distinct peer port on the same host remains
a separate endpoint. Destination validation does not prove that the endpoint
is reachable or that the remote process has the claimed identity. Wildcard
binds do not enumerate every local interface, and directed-broadcast detection
would need subnet information that this configuration does not collect.

Address mode and membership identity are independent: a named group may use
the legacy loopback ports or the explicit remote endpoint flags. Address flags
never change voting authority.

Implicit legacy groups and `--check-config` resolve and validate every
configured endpoint before opening state or listeners. Transient lookup errors
retry every 100 milliseconds; each attempt has a two-second limit and all
endpoints share a five-second deadline. Invalid successful answers are not
retried. An unavailable peer name can prevent this strict startup even if the
remaining members could form a majority. `--check-config` prints version-1
diagnostic JSON and exits without starting the node.

A named group with `--group-id` and matching `--genesis-voters` instead restores
its durable identity and membership before publishing routes or listeners.
Local advertisements must validate. Unresolved active peer routes are retained
as visible errors and retried independently; retired seed names do not block
reopening. The current durable membership chooses participants and any
replicated endpoints. Partial route availability does not grant quorum
authority. Legacy directory adoption requires the explicit migration procedure
in [membership](../membership.md).

Advertisements supply routes, not votes or HTTP redirects. Group- and
term-bound reply hints can help a stale replica contact current members without
overriding its durable membership. Clients still supply an explicit
node-to-HTTP-endpoint map.

A strict startup initializes a shared address cache from the fully validated
endpoint snapshot. Named-group startup seeds only accepted active routes and
keeps unresolved routes as explicit errors. Before a peer reconnect, resolve this node's advertisements and that
peer's endpoint through an injectable resolver. Validate the refreshed
addresses against the other peers' last accepted candidates, then atomically
update the cache. Concurrent reconnects cannot accept duplicate destinations
by checking different stale snapshots. Each resolved candidate must pass the
same destination and known-self checks as startup.

Normal refreshes keep an unrelated peer's unavailable DNS from blocking a
healthy peer's reconnect. A conflict against cached addresses triggers one full
resolution outside the cache lock. The fresh complete snapshot must pass all
validation and replace the cache by compare-and-swap against the snapshot that
caused the conflict. This permits a joint refresh when peers swap addresses;
a concurrent cache change rejects the attempt instead of overwriting newer
accepted data. An invalid refresh fails that attempt instead of silently
dialing the target's old address. A conflicting peer with unavailable DNS can
still prevent a safe refresh. Established connections are not periodically
relocated when DNS changes. A replicated membership route change cancels
removed or repointed links while retaining unchanged connections; generation
checks prevent an older in-flight lookup from republishing a superseded route.


Resolution has a two-second per-lookup deadline, a five-second deadline per
startup or ordinary reconnect snapshot, and at most sixteen candidates per
endpoint. A conflict-triggered full refresh has a separate five-second budget,
so resolution before dialing may consume up to ten seconds. The system
resolver allows at most four outstanding OS lookup workers per process and
allows at most one outstanding lookup for each normalized hostname. Other
callers for that name fail promptly while its worker remains in progress. An OS lookup cannot
be forcibly canceled: caller timeouts bound waiting, and the worker retains
its permit until the OS call returns. Detached workers keep runtime shutdown
from waiting for an unresponsive OS resolver. Distinct blocked names can still
occupy every slot; the limit bounds resource use, not DNS availability.

Connection attempts have a one-second deadline per candidate and a five-second
total dialing deadline. The starting candidate rotates between reconnect
attempts so early blackholed addresses cannot continually consume the budget
before later addresses are tried. Writes have a two-second no-progress
deadline, reset by each successful write; slow continuous progress can take
longer overall. Failed attempts use exponential backoff from 200 milliseconds
to two seconds. Dropping the final transport handle aborts its peer tasks.

The listener owns at most sixteen accepted connection tasks. At capacity it
aborts and joins the oldest before retaining the replacement. Accepted sockets
use TCP keepalive with a 60-second idle period and, on Linux/Windows, a
10-second interval; retry counts remain platform defaults. A keepalive setup
failure is logged and that connection is closed. Healthy idle follower links
have no application read-idle deadline. This trusted-small-cluster limit does
not protect against hostile connection churn or authenticate peer identities.

A coordinated shutdown closes the listening socket while retaining existing
peer streams for the durable writer's drain. Closing the writer's receiver or
aborting the listener task cancels the accepted tasks. Dropping a listener join
handle alone retains Tokio's normal detached-task behavior. Unix SIGTERM and
SIGINT, or Windows Ctrl-C and Ctrl-Break, also stop HTTP write admission. The
writer finishes started effect batches, bounds asynchronous/quorum waiting to
five seconds from the first request, and performs its required final sync. A
blocked filesystem syscall is not preempted by that budget. The binary allows
another shared 250 milliseconds for server cleanup after the writer completes.
See [shutdown behavior](../../README.md#shutdown) for completion semantics.

## Consequences

Explicit configuration can bind all interfaces while retaining a service name
as its advertised identity. Re-resolution lets a recreated service use a new
IP address. Shared cache validation catches aliases introduced by accepted DNS
changes while preserving failure independence between peers. It does not
provide an atomic DNS view of every endpoint or detect changes to an existing
connection before it reconnects. Injectable resolution makes these cases
reproducible without depending on external DNS.

Address validation preserves configuration boundaries; it does not establish
quorum authority, strengthen local HTTP reads, or add authentication. Replicated
membership changes use a separate consensus protocol. Explicit listeners belong on an isolated trusted private
network. The container smoke/fault harness applies a separate
majority-qualified, fenced local-read oracle. Configuration validation, a successful build, and
HTTP healthchecks do not establish a passing fault experiment or hosted CI run.

## Alternatives

Changing only the bind address leaves numeric-only peers unable to use service
names. Advertising a wildcard confuses local interface selection with remote
identity. Fixed container IPs couple configuration to network allocation.
Resolving once at startup permanently retains obsolete addresses after service
recreation. Validating only the selected DNS answer can hide a forbidden or
self-directed alternative. Implicitly preferring new flags over legacy flags
hides ambiguous configuration.
