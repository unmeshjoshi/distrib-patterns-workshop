# Section 2 - Why Failures Are Normal at Scale

This lab demonstrates a fundamental truth about distributed systems: **As you scale out, failures transition from rare events to constant occurrences.**

---

## The Core Insight

When you have:
- **1 disk** failing once every 3 years → failures are rare
- **10,000 disks** with the same failure rate → failures happen **every 2.6 hours!**

This mathematical reality is why distributed systems MUST treat failures as normal operations, not exceptional cases.

---

## Run the Lab

```bash
cd /opt/workshop/day1/section02-failures
python3 failure_math.py
```

---

## What the Script Demonstrates

### 1. Single Disk Failure Probability

If a disk has a Mean Time To Failure (MTTF) of **3 years**:
- Probability it fails on any given day: **0.0913%** (very low!)
- This is why a single server can run for years without hardware failure

### 2. Large-Scale System (10,000 disks)

With the same failure rate across 10,000 disks:
- Probability that **at least one** disk fails on any given day: **99.99%** (almost certain!)
- Expected failures per day: **~9 disks**
- Average time between failures: **2.6 hours**

### 3. The Solution: Replication

With replication across multiple independent servers:
- Even if failures happen every 2.6 hours somewhere in the infrastructure
- Your specific data (replicated across multiple servers) stays available
- **Distribution + Replication = Fault Tolerance**

---

## Key Takeaways

### **Failure Rate Scales Linearly with Components**

If you have:
- N components
- Each with probability `p` of failing per day
- Expected failures per day = `N × p`

**Distribution Law**: More components → more failures → **must design for failure**

### **Replication Enables Fault Tolerance**

Without replication:
- Single server failure = **complete data loss or downtime**
- Cannot tolerate ANY hardware failures

With replication (3+ copies):
- System survives individual component failures
- Can continue operating even while failures occur constantly
- **System becomes MORE reliable than any individual component**

**Availability Law**: Replication allows the system to survive failures that would otherwise be catastrophic!

---

## The Mathematics

### Single Component Failure
```
P(component fails on day D) = 1 / MTTF_days
P(component survives day D) = 1 - (1 / MTTF_days)
```

### Multiple Components (System-Wide)
```
Expected failures per day = N × p
where:
  - N = total number of components
  - p = probability of single component failing per day

P(all N components survive) = (1 - p)^N
P(at least 1 component fails) = 1 - (1 - p)^N
```

### Key Insight
As N grows large, the probability of **at least one failure** approaches 100%, even when individual failure probability `p` is very small!

---

## Next Steps

After running `failure_math.py`, think about:

1. **At what scale does failure handling become critical?**
   - With 100 disks? Time between failures?
   - With 1,000 disks? Time between failures?
   - With 10,000 disks? Time between failures?

2. **Why is replication necessary?**
   - If failures happen every 2.6 hours, how can systems stay available?
   - How many copies of data do you need?
   - Where should those copies be placed?

3. **Key Questions for Distributed Systems:**
   - How do you detect when a component fails?
   - How do you ensure replicas are consistent?
   - How do you recover from failures automatically?

---

## Connection to Distribution

This lab connects to **Section 1 (Little's Law)**:

| Section 1 | Section 2 |
|-----------|-----------|
| Why we distribute: **performance** | Why we distribute: **availability** |
| Avoid high utilization (>70%) | Handle constant failures at scale |
| Keep response times predictable | Keep system available despite failures |
| Scaling out for throughput | Replication for fault tolerance |

**Together**: Distribution + Replication = Scalable, Reliable Systems

**The Two Fundamental Reasons to Distribute:**
1. **Performance** (Section 1): Avoid the "hockey stick" curve at high utilization
2. **Availability** (Section 2): Tolerate constant failures at large scale

---

**The Pattern**: All large-scale systems treat hardware failure as a **constant**, not an **exception**.

---

**Remember**: Failures are not a problem to solve - they are a reality to design around!
