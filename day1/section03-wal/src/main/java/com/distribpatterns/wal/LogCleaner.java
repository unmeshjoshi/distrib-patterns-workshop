package com.distribpatterns.wal;

import com.distribpatterns.common.Config;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class LogCleaner {
    final Config config;
    final WriteAheadLog wal;

    public LogCleaner(Config config, WriteAheadLog wal) {
        this.config = config;
        this.wal = wal;
    }

    //TODO:When multiple logs are created in multi-raft, a thread is created for each.Make this a shared threadpool instead
    private ScheduledExecutorService singleThreadedExecutor = Executors.newScheduledThreadPool(1);
    private volatile boolean running = false;

    public void cleanLogs() {
        if (!running) {
            return;
        }
        List<WALSegment> segmentsTobeDeleted = getSegmentsToBeDeleted();
        for (WALSegment walSegment : segmentsTobeDeleted) {
            wal.removeAndDeleteSegment(walSegment);
        }
        if (running) {
            scheduleLogCleaning();
        }
    }

    abstract List<WALSegment> getSegmentsToBeDeleted();

    //<codeFragment name="logCleanerStartup">
    public void startup() {
        running = true;
        scheduleLogCleaning();
    }

    private void scheduleLogCleaning() {
        singleThreadedExecutor.schedule(() -> {
            cleanLogs();
        }, config.getCleanTaskIntervalMs(), TimeUnit.MILLISECONDS);
    }
    //</codeFragment>
    
    public void shutdown() {
        running = false;
        singleThreadedExecutor.shutdownNow();
        try {
            if (!singleThreadedExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                // Log warning if needed
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
