# Capacity Planning Lab - Little's Law Applications
## Use LLM and try variations.

These exercises help you determine **when to distribute** - the core question of distributed systems.

Use the metrics you collected from your experiments (`iostat`) to answer these questions.

---

## Quick Reference: Key Formulas

| Formula | Meaning | Units |
|---------|---------|-------|
| **L = λ × W** | Queue depth = arrival rate × response time | requests |
| **U = X × S** | Utilization = throughput × service time | 0 to 1 (0% to 100%) |
| **R = S + Wq** | Response time = service time + queue wait time | seconds |
| **Wq = S × U / (1 - U)** | Queue wait time | seconds |

**Key Insight**: When **U → 1** (100% utilization), **Wq → ∞** (queue wait explodes) - this is the "hockey stick" curve!

---

## Understanding `iostat` Output

### Standard Format: `iostat -x 1`

The workshop uses **`iostat -x 1`** which provides extended statistics. 

**iostat output** (separate read/write/discard metrics):
```
Device   r/s   w/s   rkB/s   wkB/s  r_await  w_await  aqu-sz  %util
sda     50.0  950.0  3200    60800    0.30     0.52    0.50    50.0
```

**Column meanings for Little's Law**:
- **`r/s`** = read operations per second
- **`w/s`** = write operations per second
- **`rkB/s`** = kilobytes read per second (throughput)
- **`wkB/s`** = kilobytes written per second (throughput)
- **`r_await`** = average read response time in milliseconds
- **`w_await`** = average write response time in milliseconds
- **`d_await`** = average discard/TRIM response time in milliseconds
- **`aqu-sz`** or **`avgqu-sz`** = average queue size (**L** in Little's Law)
- **`%util`** = percentage of time device was busy (**U** in Little's Law)

## Disk I/O Capacity Planning Using `iostat`

**Scenario**: You run `./disk_load.sh /tmp/io.bin 4K 60` and monitor with `iostat -x 1`.

Here's the **relevant output from `iostat -x 1`** (showing only columns that matter for Little's Law):

```
Device     r/s     w/s   r_await  w_await  aqu-sz  %util
vdb       0.00  6836.00    0.00     0.14    0.96   39.00
```

**Little's Law mapping to iostat metrics**:
- **L** (queue depth) = `aqu-sz` = **0.96 requests**
- **λ** (arrival rate) = `r/s + w/s` = 0 + 6836 = **6836 IOPS**
- **W** (response time) = `w_await` = **0.14 ms** = 0.00014 seconds (write-only workload)
- **U** (utilization) = `%util` = **39.0%**
- **Throughput** = 27400 KB/s ≈ **26.8 MB/sec** (with 4K block size)

**Key columns explained**:
- **`r/s`** = read operations per second (0 for write-only workload)
- **`w/s`** = write operations per second → **λ** (arrival rate)
- **`r_await`** = read response time in milliseconds (not applicable here)
- **`w_await`** = write response time in milliseconds → **W** (response time)
- **`aqu-sz`** = average queue size → **L** (queue depth)
- **`%util`** = disk utilization percentage → **U** (utilization)

---

### Q1. Verify Little's Law

**Question**: Using the metrics above, verify that Little's Law holds: **L = λ × W**

**Solution**:
- λ = 6836 IOPS = 6836 req/sec
- W = w_await = 0.14ms = 0.00014 seconds
- **L = λ × W = 6836 × 0.00014 = 0.957 requests**

**Answer**: Expected queue depth = **0.957 requests** ✓ (matches `aqu-sz = 0.96` from `iostat`)

**Key Insight**: Little's Law holds! The queue depth equals arrival rate times response time.

---

### Q2. Calculate Service Time

**Question**: Using the Utilization Law **U = X × S**, calculate the disk's service time per I/O operation.

**Solution**:
- U = 0.39 (39% utilization from %util)
- X = 6836 IOPS (throughput)
- **S = U / X = 0.39 / 6836 = 0.0000571 seconds ≈ 0.057ms**

**Answer**: Service time S = **0.057ms per I/O operation** (very fast NVMe SSD!)

---

### Q3. Maximum IOPS Before Saturation

**Question**: What is the maximum IOPS this disk can handle before reaching 100% utilization?

**Solution**:
- Using **U = X × S** where U = 1.0 (100% saturation)
- We know S = 0.000057 seconds (from Q2)
- **X_max = U / S = 1.0 / 0.000057 ≈ 17,544 IOPS**

**Answer**: **~17,500 IOPS** (maximum before saturation) - this is a very fast NVMe SSD!

---

### Q4. Safe Operating Threshold

**Question**: To maintain predictable response times, you want to stay below **70% disk utilization**. What is the safe maximum IOPS?

**Solution**:
- Target utilization: U = 0.70 (70%)
- Service time: S = 0.000057 seconds
- **X_safe = U / S = 0.70 / 0.000057 ≈ 12,280 IOPS**

**Answer**: **~12,300 IOPS** (safe operating threshold at 70% utilization)

---

### Q5. Response Time at High Load

**Question**: If load increases to approach saturation at 17,500 IOPS, predict the response time using **Wq = S × U / (1 - U)**

**Solution**:
- Current state: λ = 6836 IOPS, U = 39%, w_await = 0.14ms ← baseline
- At λ = 17,500 IOPS: U = X × S = 17500 × 0.000057 = **1.0 (100% saturation)**
- Using M/M/1 approximation: R = S / (1 - U)
- At U = 1.0: R → **∞** (queue explodes!)

Let's try at U = 0.95 (16,625 IOPS):
- Wq = S × U / (1 - U) = 0.000057 × 0.95 / (1 - 0.95) = 0.000054 / 0.05 = **0.00108s = 1.08ms**
- R = S + Wq = 0.057ms + 1.08ms = **1.14ms**

**Answer**: 
- Current response time (at 39% util): **0.14ms**
- Response time at 95% utilization: **1.14ms** (8× slower!) 
- At 100% utilization: **∞** (system becomes unresponsive)

**Key Insight**: This is the "hockey stick" curve - response time explodes as utilization approaches 100%!

---

### Q6. When Do You Need More Disks?

**Question**: Application requirement: Handle **30,000 write IOPS** with response time < 0.5ms. How many disks do you need?

**Solution**:
- Single disk capacity (at 70% util): 12,300 IOPS (from Q4)
- Required capacity: 30,000 IOPS
- **Disks needed**: 30,000 / 12,300 = 2.44 → **need 3 disks** (to stay at 70% util per disk)

**Answer**: **3 disks** (each handling ~10,000 IOPS at 70% utilization)


### **The Fundamental Insight**

> **Running near capacity creates exponential queueing delays.**  
> **Distribution allows us to operate multiple resources at comfortable utilization levels, maintaining predictable linear performance.**

This is the core insight of distributed systems design!

Little's Law (**L = λ × W**) proves mathematically why queueing explodes at high utilization. When U → 1, your system **will** slow down exponentially. Distribution is not optional for high-performance, scalable systems - it's inevitable.

---

**Remember**: Distribution is not about using 100% of every resource efficiently - it's about maintaining **predictable performance** by keeping utilization comfortably below saturation!

