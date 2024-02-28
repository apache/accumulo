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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
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

  /**
   * An entry point for updating tablets metadata using a conditional writer. The returned mutator
   * will buffer everything in memory until {@link ConditionalTabletsMutator#process()} is called.
   * If buffering everything in memory is undesirable, then consider using
   * {@link #conditionallyMutateTablets(Consumer)}
   *
   * @see ConditionalTabletMutator#submit(RejectionHandler)
   */
  default ConditionalTabletsMutator conditionallyMutateTablets() {
    throw new UnsupportedOperationException();
  }

  /**
   * An entry point for updating tablets metadata using a conditional writer asynchronously. This
   * will process conditional mutations in the background as they are added. The benefit of this
   * method over {@link #conditionallyMutateTablets()} is that it can avoid buffering everything in
   * memory. Using this method may also be faster as it allows tablet metadata scans and conditional
   * updates of tablets to run concurrently.
   *
   * @param resultsConsumer as conditional mutations are processed in the background their result is
   *        passed to this consumer. This consumer should be thread safe as it may be called from a
   *        different thread.
   * @return A conditional tablet mutator that will asynchronously report results. Closing this
   *         object will force everything to be processed and reported. The returned object is not
   *         thread safe and is only intended to be used by a single thread.
   * @see ConditionalTabletMutator#submit(RejectionHandler)
   */
  default AsyncConditionalTabletsMutator
      conditionallyMutateTablets(Consumer<ConditionalResult> resultsConsumer) {
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

  interface ConditionalResult {

    /**
     * This enum was created instead of using {@link ConditionalWriter.Status} because Ample has
     * automated handling for most of the statuses of the conditional writer and therefore only a
     * subset are expected to be passed out of Ample. This enum represents the subset that Ample
     * will actually return.
     */
    enum Status {
      ACCEPTED, REJECTED
    }

    /**
     * Returns the status of the conditional mutation or may return a computed status of ACCEPTED in
     * some cases, see {@link ConditionalTabletMutator#submit(RejectionHandler)} for details.
     */
    Status getStatus();

    KeyExtent getExtent();

    /**
     * This can only be called when {@link #getStatus()} returns something other than
     * {@link Status#ACCEPTED}. It reads that tablets metadata for a failed conditional mutation.
     * This can be used to see why it was not accepted.
     */
    TabletMetadata readMetadata();
  }

  interface AsyncConditionalTabletsMutator extends AutoCloseable {
    /**
     * @return A fluent interface to conditional mutating a tablet. Ensure you call
     *         {@link ConditionalTabletMutator#submit(RejectionHandler)} when finished.
     */
    OperationRequirements mutateTablet(KeyExtent extent);

    /**
     * Closing ensures that all mutations are processed and their results are reported.
     */
    @Override
    void close();
  }

  interface ConditionalTabletsMutator extends AsyncConditionalTabletsMutator {

    /**
     * After creating one or more conditional mutations using {@link #mutateTablet(KeyExtent)}, call
     * this method to process them using a {@link ConditionalWriter}
     *
     * @return The result from the {@link ConditionalWriter} of processing each tablet.
     */
    Map<KeyExtent,ConditionalResult> process();
  }

  /**
   * Interface for changing a tablets persistent data.
   */
  interface TabletUpdates<T> {
    T putPrevEndRow(Text per);

    T putFile(ReferencedTabletFile path, DataFileValue dfv);

    T putFile(StoredTabletFile path, DataFileValue dfv);

    T deleteFile(StoredTabletFile path);

    T putScan(StoredTabletFile path);

    T deleteScan(StoredTabletFile path);

    T putFlushId(long flushId);

    T putFlushNonce(long flushNonce);

    T putLocation(Location location);

    T deleteLocation(Location location);

    T putZooLock(String zookeeperRoot, ServiceLock zooLock);

    T putDirName(String dirName);

    T putWal(LogEntry logEntry);

    T deleteWal(LogEntry logEntry);

    T putTime(MetadataTime time);

    T putBulkFile(ReferencedTabletFile bulkref, FateId fateId);

    T deleteBulkFile(StoredTabletFile bulkref);

    T putSuspension(TServerInstance tserver, long suspensionTime);

    T deleteSuspension();

    T putExternalCompaction(ExternalCompactionId ecid, CompactionMetadata ecMeta);

    T deleteExternalCompaction(ExternalCompactionId ecid);

    T putCompacted(FateId fateId);

    T deleteCompacted(FateId fateId);

    T putTabletAvailability(TabletAvailability tabletAvailability);

    T setHostingRequested();

    T deleteHostingRequested();

    T putOperation(TabletOperationId opId);

    T deleteOperation();

    T putSelectedFiles(SelectedFiles selectedFiles);

    T deleteSelectedFiles();

    /**
     * Deletes all the columns in the keys.
     *
     * @throws IllegalArgumentException if rows in keys do not match tablet row or column visibility
     *         is not empty
     */
    T deleteAll(Set<Key> keys);

    T setMerged();

    T deleteMerged();

    T putUserCompactionRequested(FateId fateId);

    T deleteUserCompactionRequested(FateId fateId);
  }

  interface TabletMutator extends TabletUpdates<TabletMutator> {
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
   * A tablet operation is a mutually exclusive action that is running against a tablet. Its very
   * important that every conditional mutation specifies requirements about operations in order to
   * satisfy the mutual exclusion goal. This interface forces those requirements to specified by
   * making it the only choice available before specifying other tablet requirements or mutations.
   *
   * @see MetadataSchema.TabletsSection.ServerColumnFamily#OPID_COLUMN
   */
  interface OperationRequirements {

    /**
     * This should be used to make changes to a hosted tablet and ensure the location is as
     * expected. Hosted tablets should unload when an operation id set, but can update their
     * metadata prior to unloading.
     *
     * @see MetadataSchema.TabletsSection.ServerColumnFamily#OPID_COLUMN
     */
    ConditionalTabletMutator requireLocation(Location location);

    /**
     * Require a specific operation with a unique id is present. This would be normally be called by
     * the code executing that operation.
     */
    ConditionalTabletMutator requireOperation(TabletOperationId operationId);

    /**
     * Require that no mutually exclusive operations are running against this tablet.
     */
    ConditionalTabletMutator requireAbsentOperation();

    /**
     * Require an entire tablet is absent, so the tablet row has no columns. If the entire tablet is
     * absent, then this implies the tablet operation is also absent so there is no need to specify
     * that.
     */
    ConditionalTabletMutator requireAbsentTablet();
  }

  /**
   * Convenience interface for handling conditional mutations with a status of REJECTED.
   */
  interface RejectionHandler extends Predicate<TabletMetadata> {

    /**
     * @return true if the handler should be called when a tablet no longer exists
     */
    default boolean callWhenTabletDoesNotExists() {
      return false;
    }

    /**
     * @return a RejectionHandler that considers that case where the tablet no longer exists as
     *         accepted.
     */
    static RejectionHandler acceptAbsentTablet() {
      return new Ample.RejectionHandler() {
        @Override
        public boolean callWhenTabletDoesNotExists() {
          return true;
        }

        @Override
        public boolean test(TabletMetadata tabletMetadata) {
          return tabletMetadata == null;
        }
      };
    }
  }

  interface ConditionalTabletMutator extends TabletUpdates<ConditionalTabletMutator> {

    /**
     * Require that a tablet has no future or current location set.
     */
    ConditionalTabletMutator requireAbsentLocation();

    /**
     * Require that a tablet currently has the specified future or current location.
     */
    ConditionalTabletMutator requireLocation(Location location);

    /**
     * Requires the tablet to have the specified tablet availability before any changes are made.
     */
    ConditionalTabletMutator requireTabletAvailability(TabletAvailability tabletAvailability);

    /**
     * Requires the specified external compaction to exists
     */
    ConditionalTabletMutator requireCompaction(ExternalCompactionId ecid);

    /**
     * For the specified columns, requires the tablets metadata to be the same at the time of update
     * as what is in the passed in tabletMetadata object.
     */
    ConditionalTabletMutator requireSame(TabletMetadata tabletMetadata, ColumnType type,
        ColumnType... otherTypes);

    ConditionalTabletMutator requireAbsentLogs();

    /**
     * <p>
     * Ample provides the following features on top of the conditional writer to help automate
     * handling of edges cases that arise when using the conditional writer.
     * <ul>
     * <li>Automatically resubmit conditional mutations with a status of
     * {@link org.apache.accumulo.core.client.ConditionalWriter.Status#UNKNOWN}.</li>
     * <li>When a mutation is rejected (status of
     * {@link org.apache.accumulo.core.client.ConditionalWriter.Status#REJECTED}) it will read the
     * tablets metadata and call the passed rejectionHandler to determine if the mutation should be
     * considered as accepted.</li>
     * <li>For status of
     * {@link org.apache.accumulo.core.client.ConditionalWriter.Status#INVISIBLE_VISIBILITY} and
     * {@link org.apache.accumulo.core.client.ConditionalWriter.Status#VIOLATED} ample will throw an
     * exception. This is done so that all code does not have to deal with these unexpected
     * statuses.</li>
     * </ul>
     *
     * <p>
     * The motivation behind the rejectionHandler is to help sort things out when conditional
     * mutations are submitted twice and the subsequent submission is rejected even though the first
     * submission was accepted. There are two causes for this. First when a threads is running in
     * something like FATE it may submit a mutation and the thread dies before it sees the response.
     * Later FATE will run the code again for a second time, submitting a second mutation. The
     * second cause is ample resubmitting on unknown as mentioned above. Below are a few examples
     * that go over how Ample will handle these different situations.
     *
     * <h4>Example 1</h4>
     *
     * <ul>
     * <li>Conditional mutation CM1 with a condition requiring an absent location that sets a future
     * location is submitted. When its submitted to ample a rejectionHandler is set that checks the
     * future location.</li>
     * <li>Inside Ample CM1 is submitted to a conditional writer and returns a status of UNKNOWN,
     * but it actually succeeded. This could be caused by the mutation succeeding and the tablet
     * server dying just before it reports back.</li>
     * <li>Ample sees the UNKNOWN status and resubmits CM1 for a second time. Because the future
     * location was set, the mutation is returned to ample with a status of rejected by the
     * conditional writer.</li>
     * <li>Because the mutation was rejected, ample reads the tablet metadata and calls the
     * rejectionHandler. The rejectionHandler sees the future location was set and reports that
     * everything is ok, therefore ample reports the status as ACCEPTED.</li>
     * </ul>
     *
     * <h4>Example 2</h4>
     *
     * <ul>
     * <li>Conditional mutation CM2 with a condition requiring an absent location that sets a future
     * location is submitted. When its submitted to ample a rejectionHandler is set that checks the
     * future location.</li>
     * <li>Inside Ample CM2 is submitted to a conditional writer and returns a status of UNKNOWN,
     * but it actually never made it to the tserver. This could be caused by the tablet server dying
     * just after a network connection was established to send the mutation.</li>
     * <li>Ample sees the UNKNOWN status and resubmits CM2 for a second time. There is no future
     * location set so the mutation is returned to ample with a status of accepted by the
     * conditional writer.</li>
     * <li>Because the mutation was accepted, ample never calls the rejectionHandler and returns it
     * as accepted.</li>
     * </ul>
     *
     * <h4>Example 3</h4>
     *
     * <ul>
     * <li>Conditional mutation CM3 with a condition requiring an absent operation that sets the
     * operation id to a fate transaction id is submitted. When it's submitted to ample a
     * rejectionHandler is set that checks if the operation id equals the fate transaction id.</li>
     * <li>The thread running the fate operation dies after submitting the mutation but before
     * seeing it was actually accepted.</li>
     * <li>Later fate creates an identical mutation to CM3, lets call it CM3.2, and resubmits it
     * with the same rejection handler.</li>
     * <li>CM3.2 is rejected because the operation id is not absent.</li>
     * <li>Because the mutation was rejected, ample calls the rejectionHandler. The rejectionHandler
     * sees in the tablet metadata that the operation id is its fate transaction id and reports back
     * true</li>
     * <li>When rejectionHandler reports true, ample reports the mutation as accepted.</li>
     * </ul>
     *
     * @param rejectionHandler if the conditional mutation comes back with a status of
     *        {@link org.apache.accumulo.core.client.ConditionalWriter.Status#REJECTED} then read
     *        the tablets metadata and apply this check to see if it should be considered as
     *        {@link org.apache.accumulo.core.client.ConditionalWriter.Status#ACCEPTED} in the
     *        return of {@link ConditionalTabletsMutator#process()}. The rejection handler is only
     *        called when a tablets metadata exists. If ample reads a tablet's metadata and the
     *        tablet no longer exists, then ample will not call the rejectionHandler with null
     *        (unless {@link RejectionHandler#callWhenTabletDoesNotExists()} returns true). It will
     *        let the rejected status carry forward in this case.
     */
    void submit(RejectionHandler rejectionHandler);
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
   * @param fateId The FateId of the Bulk Import Fate operation.
   */
  default void addBulkLoadInProgressFlag(String path, FateId fateId) {
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
}
