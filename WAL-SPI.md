<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Write-ahead log storage SPI (current implementation)

Accumulo's WAL storage SPI lets a TabletServer create raw write-ahead logs using a configured
`WalProvider`. The provider owns storage of the raw byte stream; Accumulo owns the WAL format,
encryption, tablet bookkeeping, sorting, and replay. The implementation is split between the
[SPI interface][spi], the [provider registry][registry], and the built-in
[filesystem provider][dfs-provider].

## Configuration and provider identity

Two instance-wide properties configure providers in `accumulo.properties`:

| Property                 | Purpose                                                       |
| ------------------------ | ------------------------------------------------------------- |
| `instance.wal.providers` | JSON map of provider classes to their option maps.            |
| `instance.wal.provider`  | Class for **new** WALs; must be a key in the map above.       |

Both properties are required. `instance.wal.providers` defaults to an empty map and
`instance.wal.provider` defaults to an empty string. For the built-in provider, configure
`org.apache.accumulo.server.log.DfsWalProvider` with an empty option map:

```properties
instance.wal.providers={"org.apache.accumulo.server.log.DfsWalProvider":{}}
instance.wal.provider=org.apache.accumulo.server.log.DfsWalProvider
```

For a provider with options, put string-valued options in its map, for example
`{"com.example.MyWalProvider":{"endpoint":"https://wal.example"}}`. The
[distribution configuration][dist-config] shows these settings; their definitions are in
[Property.java][property].

Each implementation needs a public no-argument constructor and a stable `getId()` matching
`[A-Za-z0-9]+`. The ID `dfs` is reserved for `DfsWalProvider` and legacy filesystem WALs. The
[registry][registry] loads each configured class, passes its options to `init(InitParameters)` once per
server process, rejects duplicate provider IDs, and selects the current class by class name. The
[memoized registry][context] is accessed by the Manager, TabletServers, ScanServers, and GC. The
[Manager][manager] also checks WAL IDs in tablet metadata and ZooKeeper markers against the
configured IDs at startup.

Changing the current provider affects newly created logs only. Keep old providers in
`instance.wal.providers` while their WALs are still referenced, so recovery and cleanup can use
them. Provider IDs and configuration must remain consistent across server processes.

## Upgrade of existing WAL references

The [11-to-12 upgrader][upgrader] assigns the built-in provider ID `dfs` to pre-SPI filesystem
WALs without moving or rewriting the WAL files:

- Legacy tablet log qualifiers `-/<reference>` become `dfs/<reference>`: the upgrader adds the new
  qualifier and deletes the old one. This covers the root tablet metadata stored in ZooKeeper,
  metadata-tablet references, and user-tablet references. Already converted qualifiers are left
  alone.
- Legacy ZooKeeper WAL marker data `<state>,<reference>` becomes `<state>,dfs,<reference>`.
  Markers already containing a provider ID are retained. The upgrader checks marker format and
  verifies that an updated marker was persisted.
- The upgrader checks for remaining legacy `-/` log qualifiers in the metadata tables; it also
  verifies the root tablet's WAL references. Malformed markers or leftover legacy qualifiers cause
  the upgrade to fail rather than silently treating them as another provider's logs.

Configure `DfsWalProvider` in `instance.wal.providers` during the upgrade and retain it while old
WAL references or markers exist, even if a different class is selected in `instance.wal.provider`
for new writes. Otherwise the Manager's provider-reference check or subsequent recovery and GC
cannot handle those existing `dfs` references. The upgrade changes **reference metadata**, not the
raw WAL format or the sorted-log recovery process.

## Provider contract

[`WalProvider`][spi] exposes:

| Method                              | Use                                                   |
| ----------------------------------- | ----------------------------------------------------- |
| `create(serverAddress, UUID)`       | Exclusively create a WAL and return its `Writer`.      |
| `open(reference)`                   | Read a WAL from byte zero for sorting/recovery.        |
| `exists(reference)`                 | Check a WAL, including while it may still be written. |
| `delete(reference)`                 | Remove an unreferenced WAL; should be idempotent.      |
| `getId()` / `init(InitParameters)` | Supply the persisted ID and initialize the provider. |

The nested `Writer` supplies `getReference()`, `getOutputStream()`, `sync(Durability)`,
`getLength()`, and `close()`. It can optionally report `getCurrentReplication()` and
`getPipeline()` for diagnostics (the defaults return `-1` and an empty array). Accumulo writes the
WAL header and records to the output stream: a provider must retain the exact bytes in order and
return them through `open()`. Providers must be thread-safe for operations on different logs; a
writer must also coordinate concurrent appends and sync requests. `FLUSH` must survive a
TabletServer process failure, `SYNC` must provide the synchronization guarantee expected of synced
WAL writes, and `LOG` does not invoke `Writer.sync()`. Closing a writer must be idempotent.

The reference returned by `Writer.getReference()` is persisted and must stay usable by that
provider's `open()` and `delete()`. It must be a fully qualified Hadoop path in the standard WAL
layout, with a final `<host+port>/<uuid>` suffix (the address's `:` becomes `+`). The current
[recovery path conversion][rpath] also expects a `wal` directory immediately before the
server component; it maps `<volume>/wal/<host+port>/<uuid>` to `<volume>/recovery/<uuid>`. A custom
provider can use a custom Hadoop filesystem URI, but this path structure and access to the
corresponding recovery destination are current integration constraints.

## Writing and tracking a WAL

1. [`TabletServerLogger`][tslog] obtains `getCurrent()` from the registry when creating a
   log. Despite its name, [`DfsLogger`][dfs-logger] uses the selected provider's `create()` and
   `Writer`, so the WAL need not be stored by `DfsWalProvider`.
2. `DfsLogger` validates the returned reference as a [`LogEntry`][log-entry], writes the versioned
   header and crypto parameters, wraps the provider output stream for encryption, writes an `OPEN`
   entry, and syncs it before using the log. Subsequent tablet definitions, mutations, and
   compaction events use the existing WAL serialization. A background sync task batches durability
   requests and calls `Writer.sync(FLUSH)` or `Writer.sync(SYNC)`; it uses the optional
   replication/pipeline methods for diagnostics.
3. `LogEntry` associates the provider ID with the path. Tablet metadata stores it as a log-column
   qualifier of the form `<providerId>/<reference>`. [`WalStateManager`][wal-state] records the WAL
   state (`OPEN`, `CLOSED`, or `UNREFERENCED`), provider ID, and path in ZooKeeper WAL markers. This
   lets recovery and GC identify which provider created a given log, independently of the provider
   currently selected for new writes.

[`DfsWalProvider`][dfs-provider] is the built-in implementation. It chooses an Accumulo volume and
creates a filesystem-backed WAL under `wal/<host+port>/<uuid>` using `VolumeManager`; its writer
uses `hflush()` for `FLUSH` and `hsync()` for `SYNC`. It opens and checks files through
`VolumeManager`, and uses the filesystem trash or recursive deletion for cleanup. Where the
underlying stream is an HDFS `DFSOutputStream`, it can report replication and pipeline information.

## Recovery and garbage collection

The [Manager's `RecoveryManager`][rm] obtains the provider ID from the `LogEntry`,
derives the sibling `recovery/<uuid>` directory using `RecoveryPath`, and queues a sort. Filesystem
WALs with ID `dfs` use the configured Hadoop log closer before sorting; other providers do not use
that closer. The queued work carries the provider ID, source reference, and recovery destination.

[`LogSorter`][sorter] opens the raw source through `VolumeManager` for `dfs` or through the
registered provider's `open(reference)` for another ID. It decrypts and decodes Accumulo's WAL
records, sorts them into RFiles in the recovery directory via `VolumeManager`, and writes a finished
marker. The sorted recovery files are **not** read through `WalProvider.open()`:
[`ResolvedSortedLog`][resolved-log] validates that directory and marker,
[`RecoveryLogsIterator`][rli] opens the RFiles using the filesystem selected by
`VolumeManager`, and [`SortedLogRecovery`][slr] replays the tablet's applicable
mutations.

Thus the SPI permits alternative storage for the **raw WAL**, but it does not replace the existing
filesystem-based sorted-log recovery pipeline. HDFS itself is not a requirement of
`SortedLogRecovery`: its [unit test][slr-test] uses a local `file://` filesystem. The recovery
destination must nevertheless be accessible through `VolumeManager`, and the reference must satisfy
the path layout above.

After WALs are no longer referenced, [`GarbageCollectWriteAheadLogs`][wal-gc] deletes raw `dfs`
logs through `VolumeManager` and other raw logs through their registered provider's
`delete(reference)`. It removes sorted recovery directories through `VolumeManager` separately.
The [system configuration check][c] uses the provider's `exists(reference)` when
checking WAL markers.

[spi]: core/src/main/java/org/apache/accumulo/core/spi/wal/WalProvider.java
[registry]: server/base/src/main/java/org/apache/accumulo/server/log/WalProviderRegistry.java
[dfs-provider]: server/base/src/main/java/org/apache/accumulo/server/log/DfsWalProvider.java
[dist-config]: assemble/conf/accumulo.properties
[property]: core/src/main/java/org/apache/accumulo/core/conf/Property.java
[context]: server/base/src/main/java/org/apache/accumulo/server/ServerContext.java
[manager]: server/manager/src/main/java/org/apache/accumulo/manager/Manager.java
[upgrader]: server/manager/src/main/java/org/apache/accumulo/manager/upgrade/Upgrader11to12.java
[rpath]: server/base/src/main/java/org/apache/accumulo/server/manager/recovery/RecoveryPath.java
[tslog]: server/tserver/src/main/java/org/apache/accumulo/tserver/log/TabletServerLogger.java
[dfs-logger]: server/tserver/src/main/java/org/apache/accumulo/tserver/log/DfsLogger.java
[log-entry]: core/src/main/java/org/apache/accumulo/core/tabletserver/log/LogEntry.java
[wal-state]: server/base/src/main/java/org/apache/accumulo/server/log/WalStateManager.java
[rm]: server/manager/src/main/java/org/apache/accumulo/manager/recovery/RecoveryManager.java
[sorter]: server/tserver/src/main/java/org/apache/accumulo/tserver/log/LogSorter.java
[resolved-log]: server/tserver/src/main/java/org/apache/accumulo/tserver/log/ResolvedSortedLog.java
[rli]: server/tserver/src/main/java/org/apache/accumulo/tserver/log/RecoveryLogsIterator.java
[slr]: server/tserver/src/main/java/org/apache/accumulo/tserver/log/SortedLogRecovery.java
[slr-test]: server/tserver/src/test/java/org/apache/accumulo/tserver/log/SortedLogRecoveryTest.java
[wal-gc]: server/gc/src/main/java/org/apache/accumulo/gc/GarbageCollectWriteAheadLogs.java
[c]: server/base/src/main/java/org/apache/accumulo/server/util/checkCommand/SystemConfigCheckRunner.java
