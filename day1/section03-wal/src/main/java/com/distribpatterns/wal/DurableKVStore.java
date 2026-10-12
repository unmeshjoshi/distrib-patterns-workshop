package com.distribpatterns.wal;

import com.distribpatterns.common.Config;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DurableKVStore {
    //persistent..
    final WriteAheadLog wal;
    private final Map<String, String> kv = new HashMap<>();

    public DurableKVStore(Config config) {
        this.wal = WriteAheadLog.openWAL(config);
        applyLog();
    }

    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {
        appendLog(key, value);
        kv.put(key, value);
    }

    private Long appendLog(String key, String value) {
        Long aLong = wal.writeEntry(new SetValueCommand(key, value).serialize());
        return aLong;
    }

    public void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry : walEntries) {
            if (!walEntry.getEntryType().equals(EntryType.DATA)) {
                continue; //skip all the non data entries.
            }
            Command command = deserialize(walEntry);
            if (command instanceof SetValueCommand) {
                SetValueCommand setValueCommand = (SetValueCommand) command;
                kv.put(setValueCommand.key, setValueCommand.value);
            }
        }
    }

    private Command deserialize(WALEntry walEntry) {
        return Command.deserialize(new ByteArrayInputStream(walEntry.getData()));
    }

    //simulates crash.
    public void close() {
        wal.close();
        kv.clear();
    }

    public Collection<String> values() {
        return kv.values();
    }
}
