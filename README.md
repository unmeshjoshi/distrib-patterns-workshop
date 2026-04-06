# Patterns of Distributed Systems — Workshop Repository

A multi-day workshop exploring distributed systems patterns with hands-on implementations.

## Quick Start

```bash
# Build all projects
./gradlew build

# Build specific section
./gradlew :day1:section03-wal:build

# Run tests
./gradlew test

# Run a specific section
./gradlew :day2:section03-paxos:test

# List all Gradle projects
./gradlew projects
```

Run commands from the repository root and use the root `./gradlew` wrapper for all sections.

## Project Layout

```text
distrib-patterns-workshop/
├── day1/
│   ├── section01-littles-law/
│   ├── section02-failures/
│   ├── section03-wal/
│   └── section04-quorums/
└── day2/
    ├── section01-naive-replication/
    ├── section02-generation-voting/
    ├── section03-paxos/
    ├── section04-paxoslog/
    ├── section05-multipaxos/
    ├── section06-multipaxos-heartbeats/
    └── section07-raft/
```

## Workshop Structure

### Day 1
- **section01-littles-law** - Little's Law demonstrations
- **section02-failures** - Failure mathematics
- **section03-wal** - Write-Ahead Log implementation (Java)
- **section04-quorums** - Quorum-based consensus

### Day 2
- **section01-naive-replication** - Why single-phase replication fails, plus two-phase and three-phase execution
- **section02-generation-voting** - Leader election and fencing with generation numbers
- **section03-paxos** - Single-value Paxos with recovery and highest-generation selection
- **section04-paxoslog** - Replicated log built from per-slot Paxos
- **section05-multipaxos** - Stable-leader Multi-Paxos optimization
- **section06-multipaxos-heartbeats** - Heartbeats and leader failure detection for Multi-Paxos
- **section07-raft** - Raft leader election, log replication, and commit safety rules

See per-section READMEs for detailed instructions.

### Note on LLM usage.
Boilerplate (Gradle/Maven config, CI workflows, PlantUML, selected test scaffolding) was drafted with an LLM. Core algorithms and invariants are handwritten and covered by tests. I use AI for scaffolding only, and verify the output to refactor it by hand when needed. LLMs are great at translating and summarising the code. I encourage the users of this repo to use LLMs that way and get the most out of this code.
