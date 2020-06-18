/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.metadata;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public abstract class TabletMutatorBase implements Ample.TabletMutator {

  private final ServerContext context;
  private final KeyExtent extent;
  private final Mutation mutation;
  protected AutoCloseable closeAfterMutate;
  private boolean updatesEnabled = true;

  protected TabletMutatorBase(ServerContext ctx, KeyExtent extent) {
    this.extent = extent;
    this.context = ctx;
    mutation = new Mutation(extent.getMetadataEntry());
  }

  @Override
  public Ample.TabletMutator putPrevEndRow(Text per) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(mutation,
        KeyExtent.encodePrevEndRow(extent.getPrevEndRow()));
    return this;
  }

  @Override
  public Ample.TabletMutator putDirName(String dirName) {
    ServerColumnFamily.validateDirCol(dirName);
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mutation, new Value(dirName));
    return this;
  }

  @Override
  public Ample.TabletMutator putFile(TabletFile path, DataFileValue dfv) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(DataFileColumnFamily.NAME, path.getMetaInsertText(), new Value(dfv.encode()));
    return this;
  }

  @Override
  public Ample.TabletMutator deleteFile(StoredTabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(DataFileColumnFamily.NAME, path.getMetaUpdateDeleteText());
    return this;
  }

  @Override
  public Ample.TabletMutator putScan(TabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(ScanFileColumnFamily.NAME, path.getMetaInsertText(), new Value(new byte[0]));
    return this;
  }

  @Override
  public Ample.TabletMutator deleteScan(StoredTabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(ScanFileColumnFamily.NAME, path.getMetaUpdateDeleteText());
    return this;
  }

  @Override
  public Ample.TabletMutator putCompactionId(long compactionId) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(mutation,
        new Value(Long.toString(compactionId)));
    return this;
  }

  @Override
  public Ample.TabletMutator putFlushId(long flushId) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.ServerColumnFamily.FLUSH_COLUMN.put(mutation, new Value(Long.toString(flushId)));
    return this;
  }

  @Override
  public Ample.TabletMutator putTime(MetadataTime time) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mutation, new Value(time.encode()));
    return this;
  }

  private String getLocationFamily(LocationType type) {
    switch (type) {
      case CURRENT:
        return TabletsSection.CurrentLocationColumnFamily.STR_NAME;
      case FUTURE:
        return TabletsSection.FutureLocationColumnFamily.STR_NAME;
      case LAST:
        return TabletsSection.LastLocationColumnFamily.STR_NAME;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public Ample.TabletMutator putLocation(Ample.TServer tsi, LocationType type) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(getLocationFamily(type), tsi.getSession(), tsi.getLocation().toString());
    return this;
  }

  @Override
  public Ample.TabletMutator deleteLocation(Ample.TServer tsi, LocationType type) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(getLocationFamily(type), tsi.getSession());
    return this;
  }

  @Override
  public Ample.TabletMutator putZooLock(ZooLock zooLock) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(mutation,
        new Value(zooLock.getLockID().serialize(context.getZooKeeperRoot() + "/")));
    return this;
  }

  @Override
  public Ample.TabletMutator putWal(LogEntry logEntry) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());
    return this;
  }

  @Override
  public Ample.TabletMutator deleteWal(LogEntry logEntry) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(logEntry.getColumnFamily(), logEntry.getColumnQualifier());
    return this;
  }

  @Override
  public Ample.TabletMutator deleteWal(String wal) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(MetadataSchema.TabletsSection.LogColumnFamily.STR_NAME, wal);
    return this;
  }

  @Override
  public Ample.TabletMutator putBulkFile(TabletFile bulkref, long tid) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(TabletsSection.BulkFileColumnFamily.NAME, bulkref.getMetaInsertText(),
        new Value(FateTxId.formatTid(tid)));
    return this;
  }

  @Override
  public Ample.TabletMutator deleteBulkFile(Ample.FileMeta bulkref) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(TabletsSection.BulkFileColumnFamily.NAME, bulkref.meta());
    return this;
  }

  @Override
  public Ample.TabletMutator putChopped() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletsSection.ChoppedColumnFamily.CHOPPED_COLUMN.put(mutation, new Value("chopped"));
    return this;
  }

  protected Mutation getMutation() {
    updatesEnabled = false;
    return mutation;
  }

  public void setCloseAfterMutate(AutoCloseable closeable) {
    this.closeAfterMutate = closeable;
  }
}
