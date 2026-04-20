-------------------- MODULE BrokenStorageWriteAndValidate --------------------
(***************************************************************************)
(* BROKEN variant: Validate against storage state instead of in-memory.    *)
(*                                                                         *)
(* Bug: Acceptor reads durable state to decide whether to promise,         *)
(* then submits write, then responds after write completes.                *)
(* Between the read and write, another prepare can arrive and also         *)
(* read the same stale durable state — both promise different generations  *)
(* when only one should have been allowed.                                 *)
(*                                                                         *)
(* Correct pattern (GenerationVotingServer.accept()):                      *)
(*   private ListenableFuture<Boolean> accept(Message msg, long proposed) {*)
(*       this.generation = proposed; // update memory FIRST                *)
(*       // Immediate local update rejects any new PREPARE at this         *)
(*       // generation while the persist is still in flight.               *)
(*       return persist(GENERATION_KEY, proposed)                          *)
(*           .map(success -> { ... respond ... });                         *)
(*   }                                                                     *)
(*                                                                         *)
(* BROKEN pattern this spec models (no in-memory guard):                   *)
(*   private ListenableFuture<Boolean> accept(Message msg, long proposed) {*)
(*       return load(GENERATION_KEY)       // read from storage            *)
(*           .andThen(stored -> {                                          *)
(*               if (proposed > stored)    // stale! concurrent prepare    *)
(*                   return persist(GENERATION_KEY, proposed);             *)
(*           })                            // can read same stored value   *)
(*           .map(success -> { ... respond ... });                         *)
(*   }                                                                     *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,
    MaxGeneration

VARIABLES
    memPromised,    \* [Nodes -> Nat] tracks what's actually in memory
    durPromised,    \* [Nodes -> Nat] what's on disk
    pendingRead,    \* [Nodes -> set of {gen}] prepares waiting for storage read
    pendingDecide,  \* [Nodes -> set of [gen: Nat, readValue: Nat]] read completed, deciding
    pendingWrite,   \* [Nodes -> set of Nat] writes in flight
    responses,
    crashed

vars == <<memPromised, durPromised, pendingRead, pendingDecide, pendingWrite, responses, crashed>>

Generations == 1..MaxGeneration

---------------------------------------------------------------------------
Init ==
    /\ memPromised   = [n \in Nodes |-> 0]
    /\ durPromised   = [n \in Nodes |-> 0]
    /\ pendingRead   = [n \in Nodes |-> {}]
    /\ pendingDecide = [n \in Nodes |-> {}]
    /\ pendingWrite  = [n \in Nodes |-> {}]
    /\ responses     = {}
    /\ crashed       = [n \in Nodes |-> FALSE]

---------------------------------------------------------------------------
(* BROKEN PATTERN: read from storage to decide *)

\* Step 1: Prepare arrives. Submit storage read. Do NOT check memory.
ReceivePrepare(node, gen) ==
    /\ ~crashed[node]
    /\ pendingRead' = [pendingRead EXCEPT ![node] = @ \union {gen}]
    /\ UNCHANGED <<memPromised, durPromised, pendingDecide, pendingWrite, responses, crashed>>

\* Step 2: Storage read completes. Now we have the (possibly stale) value.
CompleteRead(node, gen) ==
    /\ ~crashed[node]
    /\ gen \in pendingRead[node]
    /\ pendingRead'   = [pendingRead   EXCEPT ![node] = @ \ {gen}]
    \* Record what we read from durable state AT THIS MOMENT
    /\ pendingDecide' = [pendingDecide EXCEPT
            ![node] = @ \union {[gen |-> gen, readValue |-> durPromised[node]]}]
    /\ UNCHANGED <<memPromised, durPromised, pendingWrite, responses, crashed>>

\* Step 3: Decide based on the (stale) read value, then submit write.
DecideAndWrite(node, req) ==
    /\ ~crashed[node]
    /\ req \in pendingDecide[node]
    /\ req.gen > req.readValue              \* decide from STALE storage read
    /\ pendingDecide' = [pendingDecide EXCEPT ![node] = @ \ {req}]
    /\ memPromised'   = [memPromised   EXCEPT ![node] = req.gen]
    /\ pendingWrite'  = [pendingWrite  EXCEPT ![node] = @ \union {req.gen}]
    /\ UNCHANGED <<durPromised, pendingRead, responses, crashed>>

\* Step 3b: Reject if stale read says no
RejectPrepare(node, req) ==
    /\ ~crashed[node]
    /\ req \in pendingDecide[node]
    /\ req.gen <= req.readValue
    /\ pendingDecide' = [pendingDecide EXCEPT ![node] = @ \ {req}]
    /\ UNCHANGED <<memPromised, durPromised, pendingRead, pendingWrite, responses, crashed>>

\* Step 4: Write completes. Respond.
CompleteWrite(node, gen) ==
    /\ ~crashed[node]
    /\ gen \in pendingWrite[node]
    /\ durPromised'  = [durPromised  EXCEPT ![node] = gen]
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = @ \ {gen}]
    /\ responses'    = responses \union {[from |-> node, gen |-> gen]}
    /\ UNCHANGED <<memPromised, pendingRead, pendingDecide, crashed>>

\* Crash
CrashAndRecover(node) ==
    /\ ~crashed[node]
    /\ crashed'       = [crashed       EXCEPT ![node] = TRUE]
    /\ memPromised'   = [memPromised   EXCEPT ![node] = durPromised[node]]
    /\ pendingRead'   = [pendingRead   EXCEPT ![node] = {}]
    /\ pendingDecide' = [pendingDecide EXCEPT ![node] = {}]
    /\ pendingWrite'  = [pendingWrite  EXCEPT ![node] = {}]
    /\ UNCHANGED <<durPromised, responses>>

Restart(node) ==
    /\ crashed[node]
    /\ crashed' = [crashed EXCEPT ![node] = FALSE]
    /\ UNCHANGED <<memPromised, durPromised, pendingRead, pendingDecide, pendingWrite, responses>>

---------------------------------------------------------------------------
Next ==
    \/ \E n \in Nodes, g \in Generations : ReceivePrepare(n, g)
    \/ \E n \in Nodes, g \in Generations : CompleteRead(n, g)
    \/ \E n \in Nodes : \E req \in pendingDecide[n] : DecideAndWrite(n, req)
    \/ \E n \in Nodes : \E req \in pendingDecide[n] : RejectPrepare(n, req)
    \/ \E n \in Nodes, g \in Generations : CompleteWrite(n, g)
    \/ \E n \in Nodes : CrashAndRecover(n)
    \/ \E n \in Nodes : Restart(n)

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
(* SAFETY PROPERTIES *)

TypeOK ==
    /\ memPromised \in [Nodes -> 0..MaxGeneration]
    /\ durPromised \in [Nodes -> 0..MaxGeneration]

\* Responses backed by durable state
ResponseImpliesDurable ==
    \A r \in responses :
        durPromised[r.from] >= r.gen

\* The REAL bug: a node can promise gen=1 and gen=2 from the same stale read,
\* leading to durable state that doesn't reflect monotonic promises.
\* Two writes in flight can complete in any order, overwriting each other.
DurableMonotonic ==
    \A n \in Nodes :
        \A w1, w2 \in pendingWrite[n] :
            \* If two writes are pending, the later one could overwrite the earlier
            \* This means durPromised could go BACKWARDS
            TRUE  \* (placeholder — the real violation shows in ResponseImpliesDurable
                  \*  when writes complete out of order)

\* The key property: no node should have two concurrent promises in flight
\* that were both decided from the same stale state
NoConcurrentPromises ==
    \A n \in Nodes :
        Cardinality(pendingWrite[n]) <= 1

=============================================================================
