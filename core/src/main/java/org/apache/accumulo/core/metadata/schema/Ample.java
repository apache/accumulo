/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.metadata.schema;

import java.util.Collection;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Accumulo Metadata Persistence Layer. Entry point and abstractions layer for reading and updating
 * persisted Accumulo metadata. This metadata may be stored in Zookeeper or in Accumulo system
 * tables.
 *
 * <p>
 * This interface seeks to satisfy the following goals.
 *
 * <UL>
 * <LI>Provide a single entry point for all reading and writing of Accumulo Metadata.
 * <LI>The root tablet persists its data in Zookeeper. Metadata tablets persist their data in root
 * tablet. All other tablets persist their data in the metadata table. This interface abstracts how
 * and where information for a tablet is actually persisted.
 * <LI>Before the creation of this interface, many concurrent metadata table updates resulted in
 * separate synchronous RPCs. The design of this interface allows batching of metadata table updates
 * within a tablet server for cluster wide efficiencies. Batching is not required by
 * implementations, but the design of the interface makes it possible.
 * <LI>Make code that updates Accumulo persistent metadata more concise. Before this interface
 * existed, there was a lot of redundant and verbose code for updating metadata.
 * <LI>Reduce specialized code for the root tablet. Currently there is specialized code to manage
 * the root tablets files that is different from all other tablets. This interface is the beginning
 * of an effort to remove this specialized code. See #936
 * </UL>
 */
public interface Ample {

  /**
   * Read a single tablets metadata. No checking is done for prev row, so it could differ.
   *
   * @param extent
   *          Reads tablet metadata using the table id and end row from this extent.
   * @param colsToFetch
   *          What tablets columns to fetch. If empty, then everything is fetched.
   */
  TabletMetadata readTablet(KeyExtent extent, ColumnType... colsToFetch);

  /**
   * Initiates mutating a single tablets persistent metadata. No data is persisted until the
   * {@code mutate()} method is called on the returned object. If updating multiple tablets,
   * consider using {@link #mutateTablets()}
   *
   * @param extent
   *          Mutates a tablet that has this table id and end row. The prev end row is not
   *          considered or checked.
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

  default void putGcCandidates(TableId tableId, Collection<? extends Ample.FileMeta> candidates) {
    throw new UnsupportedOperationException();
  }

  default void deleteGcCandidates(TableId tableId, Collection<String> paths) {
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
   * Temporary interface, place holder for some server side types like TServerInstance. Need to
   * simplify and possibly combine these type.
   */
  interface TServer {
    HostAndPort getLocation();

    String getSession();
  }

  /**
   * Temporary interface, place holder for the server side type FileRef. Need to simplify this type.
   */
  interface FileMeta {
    public Text meta();

    public Path path();
  }

  /**
   * Interface for changing a tablets persistent data.
   */
  interface TabletMutator {
    public TabletMutator putPrevEndRow(Text per);

    public TabletMutator putFile(FileMeta path, DataFileValue dfv);

    public TabletMutator putScan(FileMeta path);

    public TabletMutator deleteFile(FileMeta path);

    public TabletMutator putCompactionId(long compactionId);

    public TabletMutator putLocation(TServer tserver, LocationType type);

    public TabletMutator deleteLocation(TServer tserver, LocationType type);

    public TabletMutator putZooLock(ZooLock zooLock);

    public TabletMutator putDir(String dir);

    public TabletMutator putWal(LogEntry logEntry);

    public TabletMutator deleteWal(String wal);

    public TabletMutator deleteWal(LogEntry logEntry);

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
    public void mutate();
  }
}
