------------------------ MODULE BrokenDurableDecision ------------------------
(***************************************************************************)
(* BROKEN variant of the Durable Decision pattern.                         *)
(*                                                                         *)
(* Bug: Responds to proposer BEFORE storage write completes.               *)
(* On crash, the promise is lost but the proposer believes it holds one.   *)
(*                                                                         *)
(* TLC should find a counterexample trace for ResponseImpliesDurable.      *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,
    MaxGeneration

VARIABLES
    memPromised,
    durPromised,
    pendingWrite,
    responses,
    crashed

vars == <<memPromised, durPromised, pendingWrite, responses, crashed>>

Generations == 1..MaxGeneration

---------------------------------------------------------------------------
Init ==
    /\ memPromised  = [n \in Nodes |-> 0]
    /\ durPromised  = [n \in Nodes |-> 0]
    /\ pendingWrite = [n \in Nodes |-> 0]
    /\ responses    = {}
    /\ crashed      = [n \in Nodes |-> FALSE]

---------------------------------------------------------------------------
(* BROKEN: Respond immediately, before storage write completes *)

ReceivePrepare(node, gen) ==
    /\ ~crashed[node]
    /\ gen > memPromised[node]
    /\ memPromised'  = [memPromised  EXCEPT ![node] = gen]
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = gen]
    \* BUG: send response NOW, before storage write completes
    /\ responses'    = responses \union {[from |-> node, gen |-> gen]}
    /\ UNCHANGED <<durPromised, crashed>>

\* Storage write completes later
CompleteWrite(node) ==
    /\ ~crashed[node]
    /\ pendingWrite[node] > 0
    /\ durPromised'  = [durPromised  EXCEPT ![node] = pendingWrite[node]]
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = 0]
    /\ UNCHANGED <<memPromised, responses, crashed>>

\* Crash: lose in-memory and pending, recover from durable
CrashAndRecover(node) ==
    /\ ~crashed[node]
    /\ crashed'      = [crashed      EXCEPT ![node] = TRUE]
    /\ memPromised'  = [memPromised  EXCEPT ![node] = durPromised[node]]
    /\ pendingWrite' = [pendingWrite EXCEPT ![node] = 0]
    /\ UNCHANGED <<durPromised, responses>>

Restart(node) ==
    /\ crashed[node]
    /\ crashed' = [crashed EXCEPT ![node] = FALSE]
    /\ UNCHANGED <<memPromised, durPromised, pendingWrite, responses>>

---------------------------------------------------------------------------
Next ==
    \/ \E n \in Nodes, g \in Generations : ReceivePrepare(n, g)
    \/ \E n \in Nodes : CompleteWrite(n)
    \/ \E n \in Nodes : CrashAndRecover(n)
    \/ \E n \in Nodes : Restart(n)

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
(* Same safety properties — TLC should find a violation *)

TypeOK ==
    /\ memPromised  \in [Nodes -> 0..MaxGeneration]
    /\ durPromised  \in [Nodes -> 0..MaxGeneration]
    /\ pendingWrite \in [Nodes -> 0..MaxGeneration]

\* This WILL be violated: response sent before durable
ResponseImpliesDurable ==
    \A r \in responses :
        durPromised[r.from] >= r.gen

MemoryAheadOfDurable ==
    \A n \in Nodes :
        ~crashed[n] => memPromised[n] >= durPromised[n]

=============================================================================
