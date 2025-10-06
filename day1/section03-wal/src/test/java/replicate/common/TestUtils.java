package replicate.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Test utilities for WAL tests
 */
public class TestUtils {
    
    /**
     * Create a temporary directory for testing
     */
    public static File tempDir(String prefix) {
        try {
            File tempDir = Files.createTempDirectory(prefix.replace("/", "_")).toFile();
            tempDir.deleteOnExit();
            return tempDir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp directory", e);
        }
    }
}
