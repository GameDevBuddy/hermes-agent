# Original App/native call protocol — implementation contract v1

Backend owner: this checkout. App/Broker/GUI owner: the existing Root chat.
Intake tree: `c681a6abb4782dd0d728f2629e9cad33e25501fc`.
Status: implementation in progress; this file is not an acceptance receipt.

## Existing source handshake

Keep the existing exact `native.invoke` websocket request, `bind_native_call`
private control command and `native_call_bound` response (`executionHeld=true`).
Those authenticate source IO only. They do not execute or grant a native permit.

## Additional private commands and events

Every command uses the SAME original parent stdin reader/stdout writer and
retained AppCall. Common exact fields are `protocol`, `command`, `attemptId`,
`requestId`, `nonce`, `contextId`, `serverRequestId`, `wireSha256`,
`invocationId`, `invocationSequence`. `requestId` is a fresh ULID per phase;
`invocationId` is the original broker's lowercase UUID and `invocationSequence`
is its original positive u64. The invocation pair cannot change between phases.
The context nonce, server request and original wire must match the existing
source-bound call. Each phase is consumed before callbacks and cannot replay.

Backend prepare intake reserves ALL THREE final ACK/completion/release credits
and original control-book cells before preparation can publish a proof. Later
commands consume those exact original cells without allocating another slot or
charging again; another call/context cannot spend them. The credit record stays
retained on the AppCall when its original book cell becomes the actual phase.
Invocation IDs cannot replay and sequences strictly increase within the original
context nonce. Failure during reservation emits no prepared proof.

1. App's ACTUAL host decision for this exact call sends `prepare_native_call`.
   Backend worker acquires concrete SQLite writer exclusion, reserves the
   original SQL pin/use and emits `native_call_prepared` with `proof` (initial
   CanonicalCommitProof). No arbitrary viewer/RPC receipt may issue this phase.
2. App dispatches the native operation only after the prepared response matches
   its actual retained broker/caller/argument record and genuine initial proof.
   The original canonical websocket returns one JSON-RPC result for the SAME
   `serverRequestId`: exact result keys `invocationId`, `invocationSequence`,
   `rawResult`. `rawResult` is the exact native canonical result string, bounded
   by the existing 8 MiB text bound. Native operation failures are result data;
   transport/cancellation/unknown failures are not a successful completion.
3. Backend returns that original result to the existing agent. Its actual
   canonical result projection, existing batch transaction, marker sync and
   canonical tool observer produce the AFTER-SAVE proof. Backend emits
   `native_call_committed` with `proof` on the original private stdout.
4. App's actual queue ACK sends `ack_native_call` with common fields and
   `proofSha256` (the SHA256 of the exact canonical proof JSON, correlation only).
   Backend emits `native_call_acknowledged`.
5. App's actual completion owner sends `complete_native_call` with the same
   fields/proofSha256. Backend emits `native_call_completed`.
6. App's actual caller/queue release sends `release_native_call` with the same
   fields/proofSha256. Only then may the backend worker retire its original SQL
   pin/use and release its writer exclusion; it emits `native_call_released`.

Events repeat common fields (using `event` instead of `command`). Proof-carrying
events also include `proofSha256`; proof is serialized as ASCII JSON with sorted
keys and separators `,`/`:`. No payload proof is accepted from stdin. The App
constructs its typed receipt only inside its original coupled caller, using the
actual original private-channel response, not by trusting an arbitrary JSON hash.

`native_call_committed` repeats the ORIGINAL `prepare_native_call.requestId`
(a spontaneous event on that same retained prepare record). Prepared/committed
contain `proof` and `proofSha256`. Acknowledged/completed/released echo their own
command requestId and proofSha256, without a payload proof.

Reviewed App decline before any preparation uses the SAME original socket:
`{"jsonrpc":"2.0","id":"<original srq id>","error":{"code":-32041,"message":"User declined native request"}}`.
It returns a canonical denied-tool result and closes source-call work without a
broker invocation, SQL pin or native use. Other errors, unknowns or cancellation
are not a decline/release shortcut. Terminal native cancellation/deadline results
follow the normal result/commit/ACK/completion/release sequence.

## Proof payload

Exact top-level proof fields: `schema` (`gdb-canonical-native-commit-v1`),
`phase` (`initial` or `result`), `attemptId`, `contextId`, `nonce`,
`serverRequestId`, `wireSha256`, `invocationId`, `invocationSequence`,
`writerLeaseId`, `writerExclusion` (`sqlite-exclusive-connection`),
`canonical` (initial: the actual committed original pin capture object; result:
the actual canonical tool-row commit receipt produced after original writer
return and marker/observer validation).

The wire remains within the existing 8192-byte private-frame bound. An
oversized proof refuses and retains the original call; no partial/truncated
proof is authority. Before publishing either proof, the actual original
transaction producer and retained writer lease must validate the same database,
connection, physical SQLite exclusive locking lifetime, original owner/thread,
canonical call, original arguments and source relationships. A local pin, SQL
tuple, digest, current Job query or constructor flag alone cannot produce proof.

## Concrete implementation constraints

Writer exclusion must cover native IO, actual canonical commit and the final
ACK/completion/release sequence. Implement this as an owned SQLite exclusive
connection lifetime with canonical reads/writes routed through its original
owner; concurrent process writes/DDL and wrong same-process writers must refuse.
Do not publish an unconditional writer-exclusion boolean. Failures retain the
first actual exception/traceback, original refs and consumed credits; no replay,
refund, replacement/adoption, forced cleanup or fabricated exit.

Managed startup must compose the existing real ManagedEntry/ManagedSettlement,
sole reader/writer, current bootstrap/Job and concrete resource owners. Preserve
unmanaged behavior and unsupported-resource refusals. Kernel/native/App/UE
acceptance remains Root's fresh Hades integration work; no old roots are reused.
