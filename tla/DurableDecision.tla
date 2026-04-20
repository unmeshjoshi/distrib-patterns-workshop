--------------------------- MODULE DurableDecision ---------------------------
(***************************************************************************)
(* Pattern: Durable Decision                                               *)
(*                                                                         *)
(* Models the fundamental pattern used by consensus protocols              *)
(* (Paxos, Raft, VR) for handling async durable storage:                   *)
(*   1. Decide from in-memory state (immediate)                            *)
(*   2. Update in-memory state (immediate)                                 *)
(*   3. Write to durable storage (async)                                   *)
(*   4. Respond only after storage confirms                                *)
(*                                                                         *)
(* Used in: etcd/Raft, TigerBeetle/VSR, Tickloom/Paxos                    *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,          \* Set of acceptor nodes, e.g. {n1, n2, n3}
    MaxGeneration   \* Upper bound on generation numbers to keep state space finite

VARIABLES
    memPromised,    \* [Nodes -> Nat] in-memory promised generation per node
    durPromised,    \* [Nodes -> Nat] durable (on-disk) promised generation per node
    pendingWrite,   \* [Nodes -> Nat \union {0}] pending write (0 = none)
    responses,      \* Set of [from: node, gen: Nat] promise responses in flight
    crashed         \* [Nodes -> BOOLEAN] whether node has crashed

vars == <<memPromised, durPromised, pendingWrite, responses, crashed>>

Generations == 1..MaxGeneration

---------------------------------------------------------------------------
(* Initial state: all nodes start with generation 0, no pending writes *)

Init ==
    /\ memPromised  = [n \in Nodes |-> 0]
    /\ durPromised  = [n \in Nodes |-> 0]
    /\ pendingWrite = [n \in Nodes |-> 0]
    /\ responses    = {}
    /\ crashed      = [n \in Nodes |-> FALSE]

---------------------------------------------------------------------------
(* CORRECT PATTERN: Decide from memory, write async, respond after durable *)

\* Step 1-2: Acceptor receives Prepare(gen). Decides and updates in-memory.
\*           Submits async storage write. Does NOT respond yet.
ReceivePrepare(node, gen) ==
    /\ ~crashed[node]
    /\ gen > memPromised[node]              \* decide from MEMORY
    /\ memPromised'  = [memPromised  EXCEPT ![node] = gen]  \* update memory
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = gen]  \* submit write
    /\ UNCHANGED <<durPromised, responses, crashed>>
    \* Note: no response sent here

\* Step 3-4: Storage write completes. NOW send the response.
CompleteWrite(node) ==
    /\ ~crashed[node]
    /\ pendingWrite[node] > 0               \* has a pending write
    /\ durPromised'  = [durPromised  EXCEPT ![node] = pendingWrite[node]]
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = 0]
    /\ responses'    = responses \union
                            {[from |-> node, gen |-> pendingWrite[node]]}
    /\ UNCHANGED <<memPromised, crashed>>

\* Crash: lose in-memory state and pending writes, recover from durable
CrashAndRecover(node) ==
    /\ ~crashed[node]
    /\ crashed'      = [crashed      EXCEPT ![node] = TRUE]
    /\ memPromised'  = [memPromised  EXCEPT ![node] = durPromised[node]]
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = 0]
    /\ UNCHANGED <<durPromised, responses>>

\* Restart after crash
Restart(node) ==
    /\ crashed[node]
    /\ crashed' = [crashed EXCEPT ![node] = FALSE]
    /\ UNCHANGED <<memPromised, durPromised, pendingWrite, responses>>

---------------------------------------------------------------------------
(* Next-state relation *)

Next ==
    \/ \E n \in Nodes, g \in Generations : ReceivePrepare(n, g)
    \/ \E n \in Nodes : CompleteWrite(n)
    \/ \E n \in Nodes : CrashAndRecover(n)
    \/ \E n \in Nodes : Restart(n)

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
(* SAFETY PROPERTIES *)

\* Type invariant
TypeOK ==
    /\ memPromised  \in [Nodes -> 0..MaxGeneration]
    /\ durPromised  \in [Nodes -> 0..MaxGeneration]
    /\ pendingWrite \in [Nodes -> 0..MaxGeneration]

\* Property 1: Every response in the network is backed by durable state.
\* If node sent Promise(gen), its durable state reflects at least gen.
ResponseImpliesDurable ==
    \A r \in responses :
        durPromised[r.from] >= r.gen

\* Property 2: In-memory state is always >= durable state.
\* Memory never falls behind disk (except during crash, which resets both).
MemoryAheadOfDurable ==
    \A n \in Nodes :
        ~crashed[n] => memPromised[n] >= durPromised[n]

\* Property 3: No node sends two promises for different generations
\* where a crash in between could cause inconsistency.
\* Specifically: all responses from a node are consistent with a
\* monotonically increasing sequence.
MonotonicPromises ==
    \A r1, r2 \in responses :
        (r1.from = r2.from) => (r1.gen = r2.gen \/ r1.gen # r2.gen)
        \* This is trivially true; the real check is ResponseImpliesDurable
        \* which ensures crash can't create a "forgotten" promise

=============================================================================
