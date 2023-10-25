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

import java.util.Set;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.TabletHostingGoalUtil;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CompactedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public abstract class TabletMutatorBase<T extends Ample.TabletUpdates<T>>
    implements Ample.TabletUpdates<T> {

  private static final Value EMPTY_VALUE = new Value();

  protected final Mutation mutation;
  protected AutoCloseable closeAfterMutate;
  protected boolean updatesEnabled = true;

  @SuppressWarnings("unchecked")
  private T getThis() {
    return (T) this;
  }

  protected TabletMutatorBase(KeyExtent extent) {
    mutation = new Mutation(extent.toMetaRow());
  }

  protected TabletMutatorBase(Mutation mutation) {
    this.mutation = mutation;
  }

  @Override
  public T putPrevEndRow(Text per) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    TabletColumnFamily.PREV_ROW_COLUMN.put(mutation, TabletColumnFamily.encodePrevEndRow(per));
    return getThis();
  }

  @Override
  public T putDirName(String dirName) {
    ServerColumnFamily.validateDirCol(dirName);
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    ServerColumnFamily.DIRECTORY_COLUMN.put(mutation, new Value(dirName));
    return getThis();
  }

  @Override
  public T putFile(ReferencedTabletFile path, DataFileValue dfv) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(DataFileColumnFamily.NAME, path.insert().getMetadataText(),
        new Value(dfv.encode()));
    return getThis();
  }

  @Override
  public T putFile(StoredTabletFile path, DataFileValue dfv) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(DataFileColumnFamily.NAME, path.getMetadataText(), new Value(dfv.encode()));
    return getThis();
  }

  @Override
  public T deleteFile(StoredTabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(DataFileColumnFamily.NAME, path.getMetadataText());
    return getThis();
  }

  @Override
  public T putScan(StoredTabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(ScanFileColumnFamily.NAME, path.getMetadataText(), new Value());
    return getThis();
  }

  @Override
  public T deleteScan(StoredTabletFile path) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(ScanFileColumnFamily.NAME, path.getMetadataText());
    return getThis();
  }

  @Override
  public T putCompactionId(long compactionId) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    ServerColumnFamily.COMPACT_COLUMN.put(mutation, new Value(Long.toString(compactionId)));
    return getThis();
  }

  @Override
  public T putFlushId(long flushId) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    ServerColumnFamily.FLUSH_COLUMN.put(mutation, new Value(Long.toString(flushId)));
    return getThis();
  }

  @Override
  public T putTime(MetadataTime time) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    ServerColumnFamily.TIME_COLUMN.put(mutation, new Value(time.encode()));
    return getThis();
  }

  protected String getLocationFamily(LocationType type) {
    switch (type) {
      case CURRENT:
        return CurrentLocationColumnFamily.STR_NAME;
      case FUTURE:
        return FutureLocationColumnFamily.STR_NAME;
      case LAST:
        return LastLocationColumnFamily.STR_NAME;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public T putLocation(Location location) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(getLocationFamily(location.getType()), location.getSession(),
        location.getHostPort());
    return getThis();
  }

  @Override
  public T deleteLocation(Location location) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(getLocationFamily(location.getType()), location.getSession());
    return getThis();
  }

  @Override
  public T putZooLock(String zookeeperRoot, ServiceLock zooLock) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    ServerColumnFamily.LOCK_COLUMN.put(mutation,
        new Value(zooLock.getLockID().serialize(zookeeperRoot + "/")));
    return getThis();
  }

  @Override
  public T putWal(LogEntry logEntry) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());
    return getThis();
  }

  @Override
  public T deleteWal(LogEntry logEntry) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(logEntry.getColumnFamily(), logEntry.getColumnQualifier());
    return getThis();
  }

  @Override
  public T deleteWal(String wal) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(LogColumnFamily.STR_NAME, wal);
    return getThis();
  }

  @Override
  public T putBulkFile(ReferencedTabletFile bulkref, long tid) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(BulkFileColumnFamily.NAME, bulkref.insert().getMetadataText(),
        new Value(FateTxId.formatTid(tid)));
    return getThis();
  }

  @Override
  public T deleteBulkFile(StoredTabletFile bulkref) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(BulkFileColumnFamily.NAME, bulkref.getMetadataText());
    return getThis();
  }

  @Override
  public T putSelectedFiles(SelectedFiles selectedFiles) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(ServerColumnFamily.SELECTED_COLUMN.getColumnFamily(),
        ServerColumnFamily.SELECTED_COLUMN.getColumnQualifier(),
        new Value(selectedFiles.getMetadataValue()));
    return getThis();
  }

  @Override
  public T deleteSelectedFiles() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(ServerColumnFamily.SELECTED_COLUMN.getColumnFamily(),
        ServerColumnFamily.SELECTED_COLUMN.getColumnQualifier());
    return getThis();
  }

  @Override
  public T putSuspension(TServerInstance tServer, long suspensionTime) {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.put(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily(),
        SuspendLocationColumn.SUSPEND_COLUMN.getColumnQualifier(),
        SuspendingTServer.toValue(tServer, suspensionTime));
    return getThis();
  }

  @Override
  public T deleteSuspension() {
    Preconditions.checkState(updatesEnabled, "Cannot make updates after calling mutate.");
    mutation.putDelete(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily(),
        SuspendLocationColumn.SUSPEND_COLUMN.getColumnQualifier());
    return getThis();
  }

  @Override
  public T putExternalCompaction(ExternalCompactionId ecid, ExternalCompactionMetadata ecMeta) {
    mutation.put(ExternalCompactionColumnFamily.STR_NAME, ecid.canonical(), ecMeta.toJson());
    return getThis();
  }

  @Override
  public T deleteExternalCompaction(ExternalCompactionId ecid) {
    mutation.putDelete(ExternalCompactionColumnFamily.STR_NAME, ecid.canonical());
    return getThis();
  }

  @Override
  public T putCompacted(long fateTxId) {
    mutation.put(CompactedColumnFamily.STR_NAME, FateTxId.formatTid(fateTxId), "");
    return getThis();
  }

  @Override
  public T deleteCompacted(long fateTxId) {
    mutation.putDelete(CompactedColumnFamily.STR_NAME, FateTxId.formatTid(fateTxId));
    return getThis();
  }

  @Override
  public T putOperation(TabletOperationId opId) {
    ServerColumnFamily.OPID_COLUMN.put(mutation, new Value(opId.canonical()));
    return getThis();
  }

  @Override
  public T deleteOperation() {
    ServerColumnFamily.OPID_COLUMN.putDelete(mutation);
    return getThis();
  }

  protected Mutation getMutation() {
    updatesEnabled = false;
    return mutation;
  }

  @Override
  public T putHostingGoal(TabletHostingGoal goal) {
    HostingColumnFamily.GOAL_COLUMN.put(mutation, TabletHostingGoalUtil.toValue(goal));
    return getThis();
  }

  @Override
  public T setHostingRequested() {
    HostingColumnFamily.REQUESTED_COLUMN.put(mutation, EMPTY_VALUE);
    return getThis();
  }

  @Override
  public T deleteHostingRequested() {
    HostingColumnFamily.REQUESTED_COLUMN.putDelete(mutation);
    return getThis();
  }

  @Override
  public T deleteAll(Set<Key> keys) {
    ByteSequence row = new ArrayByteSequence(mutation.getRow());
    keys.forEach(key -> {
      Preconditions.checkArgument(key.getRowData().equals(row), "Unexpected row %s %s", row, key);
      Preconditions.checkArgument(key.getColumnVisibilityData().length() == 0,
          "Non empty column visibility %s", key);
    });

    keys.forEach(key -> {
      mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier());
    });

    return getThis();
  }

  public void setCloseAfterMutate(AutoCloseable closeable) {
    this.closeAfterMutate = closeable;
  }

}
