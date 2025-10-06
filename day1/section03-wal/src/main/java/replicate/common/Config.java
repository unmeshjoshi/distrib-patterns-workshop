package replicate.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for Write-Ahead Log
 */
public class Config {
    private final File walDir;
    private final Long maxLogSize;
    private final Long logMaxDurationMs;
    private final Long cleanTaskIntervalMs;

    // Default constructor for Jackson
    public Config() {
        this("wal", 100L * 1024L * 1024L);
    }

    public Config(String walDir) {
        this(walDir, 100L * 1024L * 1024L);
    }

    public Config(String walDir, Long maxLogSize) {
        this(walDir, maxLogSize, TimeUnit.HOURS.toMillis(24), TimeUnit.HOURS.toMillis(1));
    }

    public Config(@JsonProperty("walDir") String walDir,
                  @JsonProperty("maxLogSize") Long maxLogSize,
                  @JsonProperty("logMaxDurationMs") Long logMaxDurationMs,
                  @JsonProperty("cleanTaskIntervalMs") Long cleanTaskIntervalMs) {
        this.walDir = new File(walDir);
        this.maxLogSize = maxLogSize;
        this.logMaxDurationMs = logMaxDurationMs;
        this.cleanTaskIntervalMs = cleanTaskIntervalMs;
    }

    public File getWalDir() {
        return walDir;
    }

    public Long getMaxLogSize() {
        return maxLogSize;
    }

    public Long getLogMaxDurationMs() {
        return logMaxDurationMs;
    }

    public Long getCleanTaskIntervalMs() {
        return cleanTaskIntervalMs;
    }
}
