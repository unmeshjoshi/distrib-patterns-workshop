# Section 1 - Why Distribute? (Little's Law & Disk I/O Lab)

This lab explores how **throughput (X)**, **service time (S)**, and **utilization (U)** behave on real disk I/O systems.  
You'll use `iostat` to monitor disk performance and verify Little's Law experimentally inside a Linux Docker container.

> **Why Docker?**  
> `iostat -x` (extended device metrics like `aqu-sz`, `w_await`, `%util`) and Linux direct I/O (`dd oflag=direct`) require a Linux kernel. Running in Docker provides a consistent Linux environment on macOS and Windows.

---

## 1  Environment Setup

### Option 1: Use Pre-built Image (Recommended)

Pull the pre-built image from Docker Hub:

```bash
docker pull unmeshjoshi/distrib-patterns:distrib-patterns-workshop
docker tag unmeshjoshi/distrib-patterns:distrib-patterns-workshop perf-lab
```

---

### Option 2: Build the Container from Source

From the project root directory (`distrib-patterns-workshop/`):

```bash
docker build -t perf-lab ./env
```

---

## 2  Running the Lab (Two Separate Console Windows)

Both the monitoring tool (`iostat`) and the workload script (`disk_load.sh`) must run inside the **same container**. To achieve this, start the container with a fixed name (`--name perf-lab`) in the first window, and join into the running container from a second window using `docker exec`.

### Terminal Window 1 — Start Container & Monitor Disk I/O

From the repository root on your host machine:

```bash
docker run --rm -it --name perf-lab \
  --privileged \
  --cpus=2 \
  --memory=4g \
  -v $(pwd):/opt/workshop \
  perf-lab
```

**Flags explained:**
- `--name perf-lab` - assigns a fixed container name so other terminals can attach
- `--privileged` - allows collection of kernel and block device metrics
- `--cpus=2` - limits to 2 CPU cores
- `--memory=4g` - limits RAM to 4GB
- `-v $(pwd):/opt/workshop` - mounts the workshop repo into `/opt/workshop`

Once inside the container shell, launch `iostat`:

```bash
iostat -x 1
```

> **Tip for Clean Output:**  
> In Docker on macOS / virtualized environments, `iostat -x 1` lists several virtual block devices (`loop0`, `sr0`, `vda`, `vdb`). Writes to `/tmp` target **`vda`**. To isolate only `vda` and skip the boot summary row:
> ```bash
> iostat -x -y 1 vda
> ```

---

### Terminal Window 2 — Attach and Generate Load

Open a **second terminal window** on your host machine and connect into the running container:

```bash
docker exec -it perf-lab bash
```

Inside the container prompt, navigate to this section:

```bash
cd /opt/workshop/day1/section01-littles-law
chmod +x disk_load.sh
```

Now you are ready to run the experiments while watching Terminal 1 in real time.

---

### Alternative: Headless Background Container

If you prefer to keep both terminal windows strictly dedicated to their output:

1. **Start the container in the background (Host):**
   ```bash
   docker run -d --rm --name perf-lab --privileged -v $(pwd):/opt/workshop perf-lab sleep infinity
   ```
2. **Terminal 1 (Monitor):**
   ```bash
   docker exec -it perf-lab iostat -x -y 1 vda
   ```
3. **Terminal 2 (Load Generator):**
   ```bash
   docker exec -it perf-lab bash -c "cd /opt/workshop/day1/section01-littles-law && ./disk_load.sh /tmp/io.bin 4K 60"
   ```
4. **Clean up when done:**
   ```bash
   docker stop perf-lab
   ```

---

## 3  Disk I/O Experiments

### Experiment A: Stable Linear Zone ($L = \lambda W$)
Run light 4KB block sequential writes for 60 seconds:

```bash
./disk_load.sh /tmp/io.bin 4K 60
```

**What to observe in Terminal 1:**
- Arrival rate equals throughput (`w/s`).
- Response time (`w_await`) stays low (~0.1–0.5 ms).
- Average queue depth (`aqu-sz`) stays near 0 or 1 ($L = \lambda W$).
- Device utilization (`%util`) remains well below saturation.

---

### Experiment B: The Utilization Ceiling & Hockey-Stick Saturation
Drive the disk toward 100% saturation by running concurrent writers with larger 64KB blocks:

```bash
./disk_load.sh /tmp/io1.bin 64K 30 &
./disk_load.sh /tmp/io2.bin 64K 30 &
```

**What to observe in Terminal 1:**
- `%util` climbs toward **95%–100%**.
- Throughput (`w/s` and `wkB/s`) plateaus at the hardware limit ($X_{\\text{max}} = 1/S$).
- Queue length (`aqu-sz`) expands rapidly.
- Response time (`w_await`) spikes non-linearly (the exponential latency hockey stick).

---

## 4  Metrics Reference

| Metric in `iostat -x 1` | Little's Law Symbol | Meaning |
|-------------------------|----------------------|---------|
| **`w/s`** | $\lambda$ / $X$ | Write operations per second (Arrival Rate / Throughput) |
| **`wkB/s`** | — | Kilobytes written per second |
| **`w_await`** | $W$ or $R$ | Average write response time in milliseconds (includes queue wait) |
| **`aqu-sz`** | $L$ | Average queue length (requests in flight) |
| **`%util`** | $U$ | Device utilization percentage ($U = X \cdot S$) |

---

## Next Steps

Try the capacity planning exercises in **[lab.md](lab.md)** to see how Little's Law guides when to partition and distribute systems!
