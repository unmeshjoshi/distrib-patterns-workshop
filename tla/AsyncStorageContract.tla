----------------------- MODULE AsyncStorageContract -----------------------
(***************************************************************************)
(* Pattern: Monotonic Log Pipelining (Async Storage Contract)              *)
(*                                                                         *)
(* Models the storage contract for asynchronous write pipelines:           *)
(*   1. In-memory monotonic index reservation (1, 2, 3...)                 *)
(*   2. Asynchronous out-of-order sector persistence to physical media     *)
(*   3. Flush barrier advancing contiguous durable prefix                  *)
(*   4. Deferred client response (only ACK up to durable prefix)           *)
(*   5. Crash recovery with prefix-only scan (truncate at first hole)      *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS 
    MaxWrites   \* Bound on total writes to check (e.g. 3)

VARIABLES 
    nextIndex,      \* In-memory monotonic index allocator (1, 2, 3...)
    inFlight,       \* Set of writes currently dispatched to async storage
    diskState,      \* [1..MaxWrites -> {"None", "Written"}] physical media
    durablePrefix,  \* Highest contiguous index confirmed durable on disk
    ackedIndex,     \* Highest index for which client received an ACK
    recoveredLog,   \* Log reconstructed after crash recovery
    nodeState       \* "Running" or "Crashed"

vars == <<nextIndex, inFlight, diskState, durablePrefix, ackedIndex, recoveredLog, nodeState>>

-----------------------------------------------------------------------------
Init ==
    /\ nextIndex = 1
    /\ inFlight = {}
    /\ diskState = [i \in 1..MaxWrites |-> "None"]
    /\ durablePrefix = 0
    /\ ackedIndex = 0
    /\ recoveredLog = <<>>
    /\ nodeState = "Running"

-----------------------------------------------------------------------------
(* 1. IN-MEMORY GUARD & DISPATCH *)
(* Coordinator assigns the next monotonic index and pipelines to async I/O *)
SubmitWrite ==
    /\ nodeState = "Running"
    /\ nextIndex <= MaxWrites
    /\ inFlight' = inFlight \cup {nextIndex}
    /\ nextIndex' = nextIndex + 1
    /\ UNCHANGED <<diskState, durablePrefix, ackedIndex, recoveredLog, nodeState>>

-----------------------------------------------------------------------------
(* 2. ASYNC STORAGE CONTROLLER (PHYSICAL REORDERING) *)
(* Hardware controller can persist ANY in-flight sector in arbitrary order! *)
DiskPersistSector(idx) ==
    /\ nodeState = "Running"
    /\ idx \in inFlight
    /\ diskState' = [diskState EXCEPT ![idx] = "Written"]
    /\ inFlight' = inFlight \ {idx}
    /\ UNCHANGED <<nextIndex, durablePrefix, ackedIndex, recoveredLog, nodeState>>

-----------------------------------------------------------------------------
(* 3. FLUSH BARRIER & DURABLE PREFIX ADVANCEMENT *)
(* The storage engine detects the highest contiguous prefix on disk *)
AdvanceDurablePrefix ==
    /\ nodeState = "Running"
    /\ durablePrefix < MaxWrites
    /\ diskState[durablePrefix + 1] = "Written"
    /\ durablePrefix' = durablePrefix + 1
    /\ UNCHANGED <<nextIndex, inFlight, diskState, ackedIndex, recoveredLog, nodeState>>

-----------------------------------------------------------------------------
(* 4. DEFERRED CLIENT RESPONSE *)
(* Only ACK clients up to the verified durable prefix *)
AckClient ==
    /\ nodeState = "Running"
    /\ ackedIndex < durablePrefix
    /\ ackedIndex' = ackedIndex + 1
    /\ UNCHANGED <<nextIndex, inFlight, diskState, durablePrefix, recoveredLog, nodeState>>

-----------------------------------------------------------------------------
(* 5. CRASH & PREFIX-ONLY RECOVERY *)
(* Scanner reads sequentially from index 1. At first hole: STOP AND TRUNCATE *)
RECURSIVE ScanPrefix(_, _)
ScanPrefix(disk, idx) ==
    IF idx <= MaxWrites /\ disk[idx] = "Written"
    THEN <<idx>> \o ScanPrefix(disk, idx + 1)
    ELSE <<>>

CrashAndRecover ==
    /\ nodeState = "Running"
    /\ nodeState' = "Crashed"
    /\ inFlight' = {}
    /\ recoveredLog' = ScanPrefix(diskState, 1)
    /\ UNCHANGED <<nextIndex, diskState, durablePrefix, ackedIndex>>

Restart ==
    /\ nodeState = "Crashed"
    /\ nodeState' = "Running"
    /\ nextIndex' = Len(recoveredLog) + 1
    /\ durablePrefix' = Len(recoveredLog)
    \* On recovery, disk is truncated to remove any orphaned sectors beyond the prefix
    /\ diskState' = [i \in 1..MaxWrites |-> IF i <= Len(recoveredLog) THEN "Written" ELSE "None"]
    /\ recoveredLog' = <<>>
    /\ UNCHANGED <<inFlight, ackedIndex>>

-----------------------------------------------------------------------------
Next ==
    \/ SubmitWrite
    \/ (\E idx \in inFlight : DiskPersistSector(idx))
    \/ AdvanceDurablePrefix
    \/ AckClient
    \/ CrashAndRecover
    \/ Restart

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* SAFETY INVARIANTS *)

TypeOK ==
    /\ nextIndex \in 1..(MaxWrites + 1)
    /\ inFlight \subseteq 1..MaxWrites
    /\ durablePrefix \in 0..MaxWrites
    /\ ackedIndex \in 0..MaxWrites
    /\ nodeState \in {"Running", "Crashed"}

(* Invariant 1: An ACKed write must NEVER be lost across a crash *)
AckImpliesDurableOnRecovery ==
    (nodeState = "Crashed") => (Len(recoveredLog) >= ackedIndex)

(* Invariant 2: Recovered storage is ALWAYS a contiguous prefix (no holes) *)
NoHolesInRecoveredLog ==
    (nodeState = "Crashed") => 
        (\A i \in 1..Len(recoveredLog) : recoveredLog[i] = i)

(* Invariant 3: Pipelining never acks beyond what is durable on disk *)
NoPrematureAck ==
    ackedIndex <= durablePrefix

=============================================================================
