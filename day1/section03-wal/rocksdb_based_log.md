### Using RocksDB to store WAL

Databases like TiKV, CockroachDB, and TiDB use RocksDB 
(Or equivalent LSM storage-based kv store) as their WAL storage. 
The key is the index and the value is the log entry.
Because we need to make sure that the keys are 
stored in order in rocksdb, and should be allowed to do range
gets for the wal, the key which is long data type
is converted to a byte array as a big-endian binary.

#### Key Encoding for Ordered Storage

RocksDB stores keys in lexicographical (byte-by-byte) order. To ensure proper numeric ordering of log indices, we use big-endian encoding for the following reasons:

1. **Big-Endian Byte Order**:
    - Most significant byte (MSB) first
    - Example for 64-bit long value `0x12345678`:
      ```
      Big-endian:    12 34 56 78 00 00 00 00
      Little-endian: 00 00 00 00 78 56 34 12
      ```

2. **Why Big-Endian?**
    - Preserves natural numeric order when compared byte-by-byte
    - Example with 16-bit numbers (for simplicity):
      ```
      Number    Big-Endian
      1         00 01
      2         00 02
      255       00 FF
      256       01 00
      257       01 01
      ```
    - Lexicographical comparison of these bytes maintains the correct numeric order

3. **Range Scans**:
    - Efficient prefix-based range queries (e.g., get all entries from index 100 to 200)
    - RocksDB's prefix iterators can efficiently seek to start positions


5. **Multi-Tenant Log Storage**:
    - Store multiple logical logs in the same RocksDB instance by prefixing keys
    - Each log gets its own namespace using a prefix (e.g., group ID)
    - Example key structure: `[groupId][index]`
      ```java
      byte[] getEntryKey(int groupId, long index) {
          byte[] key = new byte[12]; // 4 bytes for groupId + 8 bytes for index
          // Big-endian groupId
          key[0] = (byte)(groupId >>> 24);
          key[1] = (byte)(groupId >>> 16);
          key[2] = (byte)(groupId >>> 8);
          key[3] = (byte) groupId;
          // Big-endian index
          for (int i = 0; i < 8; i++) {
              key[4 + i] = (byte)(index >>> (56 - 8 * i));
          }
          return key;
      }
      ```
    - Enables efficient per-group operations:
        - Range queries within a group
        - Group-specific truncation
        - Atomic operations per group

6. **Real-World Usage**:
    - **TiKV**: Uses similar approach for Raft logs with group IDs
    - **CockroachDB**: Uses range deletes for log truncation
    - **Dragonboat**: Implements multi-group logs with prefix-based separation