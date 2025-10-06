# Section 1 - Why Distribute? (Little's Law & Disk I/O Lab)

This lab explores how **throughput (X)**, **service time (S)**, and **utilization (U)** behave on real disk I/O systems.  
You'll use `iostat` to monitor disk performance and verify Little's Law experimentally inside a Docker container.

---

## 1  Environment Setup

### Option 1: Use Pre-built Image (Recommended)

Pull the pre-built image from Docker Hub:

```bash
docker pull unmeshjoshi/distrib-patterns:distrib-patterns-workshop
docker tag unmeshjoshi/distrib-patterns:distrib-patterns-workshop perf-lab
```

This saves time by using a pre-configured image with all tools installed.

---

### Option 2: Build the Container

If you prefer to build from source, from the project root directory (`distrib-patterns-workshop/`):

```bash
docker build -t perf-lab ./env
```

### Start the Container
**Important**: Run this from the project root directory (`distrib-patterns-workshop/`)

```bash
docker run --rm -it --name perf-lab \
  --privileged \
  --cpus=2 \
  --memory=4g \
  -v $(pwd):/opt/workshop \
  perf-lab
```

**Flags explained:**
- `--privileged` - allows `mpstat`, `iostat`, `sar` to collect kernel stats
- `--cpus=2` - limit to 2 CPU cores (adjust based on your system)
- `--memory=4g` - limit to 4GB RAM (adjust to test memory pressure)
- `-v $(pwd):/opt/workshop` - mount current directory into container

Inside the container:
```bash
cd /opt/workshop/day1/section01-littles-law
chmod +x *.sh
```

---

## 2  Installed Tools

| Tool | Metric | Meaning |
|------|---------|----------|
| `iostat -x 1` | r/s w/s r_await w_await aqu-sz %util | IOPS, Response time, Queue depth, Utilization |

> **Note**: The `1` parameter means report every 1 second, giving real-time updates.

---

## 3  Disk I/O Experiment

### Run the Experiment
```bash
# In terminal 1: Monitor disk I/O
iostat -x 1

# In terminal 2: Generate disk load
./disk_load.sh /tmp/io.bin 4K 60
```

Once you see the iostat output, try the capacity planning exercises in **[lab.md](lab.md)** to learn when to distribute your systems!