# Direct I/O with O_DIRECT

Many high-performance storage engines use **O_DIRECT** to bypass the operating system's page cache entirely, giving applications direct control over I/O operations and memory management.

### What is O_DIRECT?

O_DIRECT instructs the kernel to bypass the page cache, transferring data directly between user space buffers and storage devices.

**I/O Path Comparison**:
- **Traditional I/O**: Application → Page Cache → Storage Device
- **Direct I/O**: Application → Storage Device (no intermediate caching)

### Benefits of Direct I/O

#### 1. Predictable Performance
**Page Cache Variability**:
- Cache hit: ~100μs
- Cache miss: ~1-10ms  
- Dirty page eviction: ~50-100ms (unpredictable spikes!)

**Direct I/O Consistency**: Always talks directly to storage (~1-5ms consistently)

#### 2. Memory Efficiency
- **Page Cache**: User buffer (8KB) + Page cache (8KB) = 16KB memory usage
- **Direct I/O**: User buffer (8KB) only = 50% memory saving

#### 3. CPU Efficiency
**Page Cache Overhead**:
- Memory copying between user/kernel space
- Page management (LRU updates, dirty tracking)
- Writeback coordination

### Drawbacks and Considerations

#### 1. Alignment Requirements
**Critical Constraint**: ALL parameters must be 512-byte aligned:
- Buffer memory address
- Buffer size  
- File offset

**Violation Result**: EINVAL error that crashes operations.

#### 2. No Automatic Read-Ahead
**Lost Optimization**: Page cache automatically reads ahead for sequential access (4KB request → 128KB actual read).

**Solution**: Implement application-level read-ahead strategies.

### When to Use Direct I/O

#### Ideal Use Cases
1. **WAL/Journal workloads**: Sequential writes, large batches, predictable I/O sizes
2. **Database storage engines**: Application-managed buffer pools, custom replacement policies
3. **Avoid double-caching**: When application has its own caching layer

#### When to Avoid Direct I/O
1. **Small random I/O**: Direct I/O overhead exceeds benefits for <4KB operations
2. **Cacheable workloads**: Configuration files, metadata, frequently re-read data