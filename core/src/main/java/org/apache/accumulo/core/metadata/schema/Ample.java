/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;

/**
 * Accumulo Metadata Persistence Layer. Entry point and abstractions layer for reading and updating
 * persisted Accumulo metadata. This metadata may be stored in Zookeeper or in Accumulo system
 * tables.
 *
 * <p>
 * This interface seeks to satisfy the following goals.
 *
 * <ul>
 * <li>Provide a single entry point for all reading and writing of Accumulo Metadata.
 * <li>The root tablet persists its data in Zookeeper. Metadata tablets persist their data in root
 * tablet. All other tablets persist their data in the metadata table. This interface abstracts how
 * and where information for a tablet is actually persisted.
 * <li>Before the creation of this interface, many concurrent metadata table updates resulted in
 * separate synchronous RPCs. The design of this interface allows batching of metadata table updates
 * within a tablet server for cluster wide efficiencies. Batching is not required by
 * implementations, but the design of the interface makes it possible.
 * <li>Make code that updates Accumulo persistent metadata more concise. Before this interface
 * existed, there was a lot of redundant and verbose code for updating metadata.
 * <li>Reduce specialized code for the root tablet. Currently there is specialized code to manage
 * the root tablets files that is different from all other tablets. This interface is the beginning
 * of an effort to remove this specialized code. See #936
 * </ul>
 */
public interface Ample {

  /**
   * Accumulo is a distributed tree with three levels. This enum is used to communicate to Ample
   * that code is interested in operating on the metadata of a data level. Sometimes tables ids or
   * key extents are passed to Ample in lieu of a data level, in these cases the data level is
   * derived from the table id.
   */
  public enum DataLevel {
    ROOT(null, null),
    METADATA(AccumuloTable.ROOT.tableName(), AccumuloTable.ROOT.tableId()),
    USER(AccumuloTable.METADATA.tableName(), AccumuloTable.METADATA.tableId());

    private final String table;
    private final TableId id;

    private DataLevel(String table, TableId id) {
      this.table = table;
      this.id = id;
    }

    /**
     * @return The name of the Accumulo table in which this data level stores its metadata.
     */
    public String metaTable() {
      if (table == null) {
        throw new UnsupportedOperationException();
      }
      return table;
    }

    /**
     * @return The Id of the Accumulo table in which this data level stores its metadata.
     */
    public TableId metaTableId() {
      if (id == null) {
        throw new UnsupportedOperationException();
      }
      return id;
    }

    public static DataLevel of(TableId tableId) {
      if (tableId.equals(AccumuloTable.ROOT.tableId())) {
        return DataLevel.ROOT;
      } else if (tableId.equals(AccumuloTable.METADATA.tableId())) {
        return DataLevel.METADATA;
      } else {
        return DataLevel.USER;
      }
    }
  }

  /**
   * Controls how Accumulo metadata is read. Currently this only impacts reading the root tablet
   * stored in Zookeeper. Reading data stored in the Accumulo metadata table is always immediate
   * consistency.
   */
  public enum ReadConsistency {
    /**
     * Read data in a way that is slower, but should always yield the latest data. In addition to
     * being slower, it's possible this read consistency can place higher load on shared resource
     * which can negatively impact an entire cluster.
     */
    IMMEDIATE,
    /**
     * Read data in a way that may be faster but may yield out of date data.
     */
    EVENTUAL
  }

  /**
   * Enables status based processing of GcCandidates.
   */
  public enum GcCandidateType {
    /**
     * Candidates which have corresponding file references still present in tablet metadata.
     */
    INUSE,
    /**
     * Candidates that have no matching file references and can be removed from the system.
     */
    VALID,
    /**
     * Candidates that are malformed.
     */
    INVALID
  }

  /**
   * Read a single tablets metadata. No checking is done for prev row, so it could differ. The
   * method will read the data using {@link ReadConsistency#IMMEDIATE}.
   *
   * @param extent Reads tablet metadata using the table id and end row from this extent.
   * @param colsToFetch What tablets columns to fetch. If empty, then everything is fetched.
   */
  default TabletMetadata readTablet(KeyExtent extent, ColumnType... colsToFetch) {
    return readTablet(extent, ReadConsistency.IMMEDIATE, colsToFetch);
  }

  /**
   * Read a single tablets metadata. No checking is done for prev row, so it could differ.
   *
   * @param extent Reads tablet metadata using the table id and end row from this extent.
   * @param readConsistency Controls how the data is read.
   * @param colsToFetch What tablets columns to fetch. If empty, then everything is fetched.
   */
  TabletMetadata readTablet(KeyExtent extent, ReadConsistency readConsistency,
      ColumnType... colsToFetch);

  /**
   * Entry point for reading multiple tablets' metadata. Generates a TabletsMetadata builder object
   * and assigns the AmpleImpl client to that builder object. This allows readTablets() to be called
   * from a ClientContext. Associated methods of the TabletsMetadata Builder class are used to
   * generate the metadata.
   */
  TabletsMetadata.TableOptions readTablets();

  /**
   * Initiates mutating a single tablets persistent metadata. No data is persisted until the
   * {@code mutate()} method is called on the returned object. If updating multiple tablets,
   * consider using {@link #mutateTablets()}
   *
   * @param extent Mutates a tablet that has this table id and end row. The prev end row is not
   *        considered or checked.
   */
  default TabletMutator mutateTablet(KeyExtent extent) {
    throw new UnsupportedOperationException();
  }

  /**
   * Use this when updating multiple tablets. Ensure the returns TabletsMutator is closed, or data
   * may not be persisted.
   */
  default TabletsMutator mutateTablets() {
    throw new UnsupportedOperationException();
  }

  default void putGcCandidates(TableId tableId, Collection<StoredTabletFile> candidates) {
    throw new UnsupportedOperationException();
  }

  /**
   * Unlike {@link #putGcCandidates(TableId, Collection)} this takes file and dir GC candidates.
   */
  default void putGcFileAndDirCandidates(TableId tableId, Collection<ReferenceFile> candidates) {
    throw new UnsupportedOperationException();
  }

  /**
   * Enum added to support unique candidate deletions in 2.1
   */
  default void deleteGcCandidates(DataLevel level, Collection<GcCandidate> candidates,
      GcCandidateType type) {
    throw new UnsupportedOperationException();
  }

  default Iterator<GcCandidate> getGcCandidates(DataLevel level) {
    throw new UnsupportedOperationException();
  }

  default void
      putExternalCompactionFinalStates(Collection<ExternalCompactionFinalState> finalStates) {
    throw new UnsupportedOperationException();
  }

  default Stream<ExternalCompactionFinalState> getExternalCompactionFinalStates() {
    throw new UnsupportedOperationException();
  }

  default void
      deleteExternalCompactionFinalStates(Collection<ExternalCompactionId> statusesToDelete) {
    throw new UnsupportedOperationException();
  }

  /**
   * Return an encoded delete marker Mutation to delete the specified TabletFile path. A
   * ReferenceFile is used for the parameter because the Garbage Collector is optimized to store a
   * directory for Tablet File. Otherwise, a {@link ReferencedTabletFile} object could be used. The
   * tabletFilePathToRemove is validated and normalized before creating the mutation.
   *
   * @param tabletFilePathToRemove String full path of the TabletFile
   * @return Mutation with encoded delete marker
   */
  default Mutation createDeleteMutation(ReferenceFile tabletFilePathToRemove) {
    throw new UnsupportedOperationException();
  }

  /**
   * This interface allows efficiently updating multiple tablets. Unless close is called, changes
   * may not be persisted.
   */
  public interface TabletsMutator extends AutoCloseable {
    TabletMutator mutateTablet(KeyExtent extent);

    @Override
    void close();
  }

  /**
   * Interface for changing a tablets persistent data.
   */
  interface TabletMutator {
    TabletMutator putPrevEndRow(Text per);

    TabletMutator putFile(ReferencedTabletFile path, DataFileValue dfv);

    TabletMutator putFile(StoredTabletFile path, DataFileValue dfv);

    TabletMutator deleteFile(StoredTabletFile path);

    TabletMutator putScan(StoredTabletFile path);

    TabletMutator deleteScan(StoredTabletFile path);

    TabletMutator putCompactionId(long compactionId);

    TabletMutator putFlushId(long flushId);

    TabletMutator putLocation(Location location);

    TabletMutator deleteLocation(Location location);

    TabletMutator putZooLock(ServiceLock zooLock);

    TabletMutator putDirName(String dirName);

    TabletMutator putWal(LogEntry logEntry);

    TabletMutator deleteWal(LogEntry wal);

    TabletMutator putTime(MetadataTime time);

    TabletMutator putBulkFile(ReferencedTabletFile bulkref, long tid);

    TabletMutator deleteBulkFile(StoredTabletFile bulkref);

    TabletMutator putSuspension(TServerInstance tserver, long suspensionTime);

    TabletMutator deleteSuspension();

    TabletMutator putExternalCompaction(ExternalCompactionId ecid,
        ExternalCompactionMetadata ecMeta);

    TabletMutator deleteExternalCompaction(ExternalCompactionId ecid);

    /**
     * This method persist (or queues for persisting) previous put and deletes against this object.
     * Unless this method is called, previous calls will never be persisted. The purpose of this
     * method is to prevent partial changes in the case of an exception.
     *
     * <p>
     * Implementors of this interface should ensure either all requested changes are persisted or
     * none.
     *
     * <p>
     * After this method is called, calling any method on this object will result in an exception.
     */
    void mutate();
  }

  /**
   * Insert ScanServer references to Tablet files
   *
   * @param scanRefs set of scan server ref table file objects
   */
  default void putScanServerFileReferences(Collection<ScanServerRefTabletFile> scanRefs) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get ScanServer references to Tablet files
   *
   * @return stream of scan server references
   */
  default Stream<ScanServerRefTabletFile> getScanServerFileReferences() {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete the set of scan server references
   *
   * @param refsToDelete set of scan server references to delete
   */
  default void deleteScanServerFileReferences(Collection<ScanServerRefTabletFile> refsToDelete) {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete scan server references for this server
   *
   * @param serverAddress address of server, cannot be null
   * @param serverSessionId server session id, cannot be null
   */
  default void deleteScanServerFileReferences(String serverAddress, UUID serverSessionId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a Bulk Load In Progress flag in the metadata table
   *
   * @param path The bulk directory filepath
   * @param fateTxid The id of the Bulk Import Fate operation.
   */
  default void addBulkLoadInProgressFlag(String path, long fateTxid) {
    throw new UnsupportedOperationException();
  }

  /**
   * Remove a Bulk Load In Progress flag from the metadata table.
   *
   * @param path The bulk directory filepath
   */
  default void removeBulkLoadInProgressFlag(String path) {
    throw new UnsupportedOperationException();
  }

  /**
   * Remove all the Bulk Load transaction ids from a given table's metadata
   *
   * @param tableId Table ID for transaction removals
   * @param tid Transaction ID to remove
   * @param firstSplit non-inclusive table split point at which to start looking for load markers
   * @param lastSplit inclusive tablet split point at which to stop looking for load markers
   */
  default void removeBulkLoadEntries(TableId tableId, long tid, Text firstSplit, Text lastSplit) {
    throw new UnsupportedOperationException();
  }

}
