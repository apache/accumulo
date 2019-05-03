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

package org.apache.accumulo.server.metadata;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.io.Text;

/**
 * An abstraction layer for mutating Accumulo's persistent metadata. This metadata may be stored in
 * Zookeeper or Accumulo's own metadata table.
 *
 * <p>
 * Mutators created by this interface may only queue changes for persisting. There is no guarantee
 * that any change is persisted until this object is closed.
 *
 * <p>
 * This interface seeks to satisfy the following goals.
 *
 * <UL>
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
public interface MetadataMutator extends AutoCloseable {

  // TODO remove if not useful for testing purposes.
  public interface Factory {
    MetadataMutator newMetadataMutator();
  }

  /**
   * Interface for changing a tablets persistent data.
   */
  public interface TabletMutator {
    public TabletMutator putPrevEndRow(Text per);

    public TabletMutator putFile(FileRef path, DataFileValue dfv);

    public TabletMutator putScan(FileRef path);

    public TabletMutator deleteFile(FileRef path);

    public TabletMutator putCompactionId(long compactionId);

    public TabletMutator putLocation(HostAndPort location, String session, LocationType future);

    public TabletMutator putLocation(TServerInstance tsi, LocationType future);

    public TabletMutator deleteLocation(String session, LocationType type);

    public TabletMutator deleteLocation(TServerInstance tsi, LocationType type);

    public TabletMutator putZooLock(ZooLock zooLock);

    public TabletMutator putDir(String dir);

    public TabletMutator putWal(LogEntry logEntry);

    // TODO parameter type
    public TabletMutator deleteWal(String wal);

    public TabletMutator deleteWal(LogEntry logEntry);

    /**
     * This method persist (or queues for persisting) previous put and deletes against this object.
     * Unless this method is called, previous calls will never be persisted. The purpose of this
     * method is to prevent partial changes in the case of an exception.
     *
     * <p>
     * Since this method may only queue for persisting, closing the originating
     * {@link MetadataMutator} ensures queued changes are persisted.
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

  /**
   * Interface for adding and removing file delete markers used for garbage collection. Each time a
   * method is called it may immediately persist changes, or queue them for persisting. To be sure
   * changes are persisted, the {@link MetadataMutator} that created this must be closed.
   */
  public interface GcMutator {
    public void put(FileRef path);

    public void delete(String path);
  }

  TabletMutator mutateTablet(KeyExtent extent);

  GcMutator mutateDeletes(TableId tableId);

  @Override
  void close();

  void flush();
}
