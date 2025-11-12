package com.distribpatterns.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Locale;
import java.util.zip.CRC32C;

/**
 * AtomicPersistExample
 *
 * Persists a Paxos state (promisedBallot, acceptedBallot, acceptedCommand ~7KB)
 * with a SINGLE logical write followed by fsync (ch.force(true)).
 *
 * Why useful:
 * - At the block layer (e.g., NVMe 4Kn), this one 7KB write usually becomes
 *   TWO device writes (4KiB + 4KiB). A power loss between them can persist
 *   only a partial update (half-old, half-new).
 * - The file format includes header & trailer CRC so torn/partial writes are detectable.
 *
 * Commands:
 *   java AtomicPersistExample write [--file paxos.state] [--promised 101] [--accepted 202] [--payloadKB 7]
 *   java AtomicPersistExample validate [--file paxos.state]
 *   java AtomicPersistExample dump [--file paxos.state]
 *
 * Tips:
 * - Run `trace-cmd` or `blktrace` while executing "write" to observe two WRITE requests.
 * - To demonstrate torn writes deterministically, pair this with a fault injector (e.g., dm-flakey)
 *   or your own simulated "tearing" layer.
 */
public class AtomicPersistExample {

    // ----- File format (little-endian) -----
    //  Offset  Size  Field
    //  0       4     magic = 0xPAx05E (arbitrary tag)
    //  4       4     payloadLen (N)
    //  8       8     promisedBallot
    //  16      8     acceptedBallot
    //  24      4     headerCRC32C (over payload only)
    //  28      N     payload bytes (acceptedCommand)
    //  28+N    4     trailerCRC32C (duplicate of header CRC)
    //
    // Validation rule:
    // - headerCRC32C == crc(payload)
    // - trailerCRC32C == headerCRC32C
    //
    // Any mismatch => torn/partial/corrupt file.

    private static final int MAGIC = 0x50415845; // 'PAXE' ASCII tag (fits in 32-bit)
    private static final int HEADER_FIXED = 4 + 4 + 8 + 8 + 4; // magic + len + promised + accepted + headerCRC
    private static final int TRAILER = 4;

    // ---- Commands ----
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(1);
        }
        String cmd = args[0].toLowerCase(Locale.ROOT);
        var opts = new Opts(Arrays.copyOfRange(args, 1, args.length));

        switch (cmd) {
            case "write" -> doWrite(opts);
            case "validate" -> doValidate(opts);
            case "dump" -> doDump(opts);
            default -> {
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.out.println("""
            Usage:
              java AtomicPersistExample write [--file paxos.state] [--promised 101] [--accepted 202] [--payloadKB 7]
              java AtomicPersistExample validate [--file paxos.state]
              java AtomicPersistExample dump [--file paxos.state]

            Examples:
              javac AtomicPersistExample.java
              # Single 7KB write + fsync (observe split into two 4KiB writes via trace-cmd)
              sudo trace-cmd record -e block:block_rq_issue -e block:block_rq_complete \
                                    -e nvme:nvme_setup_cmd -e nvme:nvme_complete_rq \
                                    -- java AtomicPersistExample write --file paxos.state --promised 101 --accepted 202 --payloadKB 7
              sudo trace-cmd report | egrep ' WRITE|WS|WF|FLUSH'

              # Validate on next startup (detect torn/partial)
              java AtomicPersistExample validate --file paxos.state

              # Dump human-readable fields
              java AtomicPersistExample dump --file paxos.state
            """);
    }

    // ---- write ----
    private static void doWrite(Opts o) throws IOException {
        Path file = Paths.get(o.file);
        long promised = o.promised;
        long accepted = o.accepted;
        int payloadKB = o.payloadKB;

        byte[] payload = new byte[payloadKB * 1024];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i & 0xFF);

        ByteBuffer record = encode(promised, accepted, payload);

        try (FileChannel ch = FileChannel.open(
                file,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Single logical write:
            while (record.hasRemaining()) ch.write(record);

            // fsync: make data + metadata durable (for this file)
            ch.force(true);

        }
        System.out.printf("Wrote %d bytes to %s (promised=%d, accepted=%d, payload=%d bytes)%n",
                HEADER_FIXED + payload.length + TRAILER, file, promised, accepted, payload.length);
        System.out.println("NOTE: Although this was ONE logical write, the block layer typically emitted TWO 4KiB device writes.");
    }

    // ---- validate ----
    private static void doValidate(Opts o) throws IOException {
        Path file = Paths.get(o.file);
        var read = readAll(file);
        var res = validate(read);
        System.out.printf("Validate %s: %s%n", file, res.okResult ? "OK" : "CORRUPT");
        if (!res.okResult) {
            System.out.printf("Reason: %s%n", res.reason);
            if (res.headerCrc != null && res.trailerCrc != null) {
                System.out.printf("Header CRC:  0x%08x%n", res.headerCrc);
                System.out.printf("Trailer CRC: 0x%08x%n", res.trailerCrc);
            }
        }
    }

    // ---- dump ----
    private static void doDump(Opts o) throws IOException {
        Path file = Paths.get(o.file);
        ByteBuffer buf = readAll(file).order(ByteOrder.LITTLE_ENDIAN);

        if (buf.remaining() < HEADER_FIXED + TRAILER) {
            System.out.println("File too small to contain a valid record.");
            return;
        }
        int magic = buf.getInt(0);
        int len   = buf.getInt(4);
        long promised = buf.getLong(8);
        long accepted = buf.getLong(16);
        int headerCrc = buf.getInt(24);
        int trailerCrc = buf.getInt(HEADER_FIXED + len);

        System.out.printf("File: %s%n", file);
        System.out.printf("magic=0x%08x, len=%d, promised=%d, accepted=%d%n",
                magic, len, promised, accepted);
        System.out.printf("crc(header)=0x%08x, crc(trailer)=0x%08x%n",
                headerCrc, trailerCrc);
        System.out.printf("payload[0..min(32,len)]: ");
        int peek = Math.min(32, len);
        for (int i = 0; i < peek; i++) {
            int b = Byte.toUnsignedInt(buf.get(HEADER_FIXED + i));
            System.out.printf("%02x ", b);
        }
        System.out.println();
    }

    // ---- encoding / validation helpers ----
    private static ByteBuffer encode(long promisedBallot, long acceptedBallot, byte[] payload) {
        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        int crc = (int) crc32c.getValue();

        int total = HEADER_FIXED + payload.length + TRAILER;
        ByteBuffer buf = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(MAGIC);
        buf.putInt(payload.length);
        buf.putLong(promisedBallot);
        buf.putLong(acceptedBallot);
        buf.putInt(crc);               // header CRC
        buf.put(payload);
        buf.putInt(crc);               // trailer CRC
        buf.flip();
        return buf;
    }

    private static ValidationResult validate(ByteBuffer buf) {
        buf = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        if (buf.remaining() < HEADER_FIXED + TRAILER)
            return ValidationResult.fail("record too small");

        int magic = buf.getInt(0);
        if (magic != MAGIC) return ValidationResult.fail("bad magic");

        int len = buf.getInt(4);
        if (len < 0) return ValidationResult.fail("negative length");
        if (buf.remaining() < HEADER_FIXED + len + TRAILER)
            return ValidationResult.fail("truncated record");

        long promised = buf.getLong(8);
        long accepted = buf.getLong(16);
        int headerCrc = buf.getInt(24);

        byte[] payload = new byte[len];
        buf.position(HEADER_FIXED);
        buf.get(payload, 0, len);

        int trailerCrc = buf.getInt(HEADER_FIXED + len);

        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payload.length);
        int calc = (int) crc32c.getValue();

        if (headerCrc != calc || trailerCrc != calc) {
            return ValidationResult.fail("crc mismatch (torn/partial write?)", headerCrc, trailerCrc);
        }
        // Optionally sanity-check ballots or payload semantics here
        return ValidationResult.ok();
    }

    private static ByteBuffer readAll(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = ch.size();
            if (size > Integer.MAX_VALUE) throw new IOException("file too large for this demo");
            ByteBuffer dst = ByteBuffer.allocate((int) size);
            int n = ch.read(dst);
            if (n < size) throw new IOException("short read");
            dst.flip();
            return dst;
        }
    }

    // ---- tiny CLI opts ----
    private static final class Opts {
        String file = "paxos.state";
        long promised = 101L;
        long accepted = 202L;
        int payloadKB = 7;

        Opts(String[] args) {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--file" -> file = args[++i];
                    case "--promised" -> promised = Long.parseLong(args[++i]);
                    case "--accepted" -> accepted = Long.parseLong(args[++i]);
                    case "--payloadKB" -> payloadKB = Integer.parseInt(args[++i]);
                    default -> { /* ignore unknown */ }
                }
            }
        }
    }

    private record ValidationResult(boolean okResult, String reason, Integer headerCrc, Integer trailerCrc) {
        static ValidationResult ok() { return new ValidationResult(true, null, null, null); }
        static ValidationResult fail(String r) { return new ValidationResult(false, r, null, null); }
        static ValidationResult fail(String r, int header, int trailer) { return new ValidationResult(false, r, header, trailer); }
    }
}
