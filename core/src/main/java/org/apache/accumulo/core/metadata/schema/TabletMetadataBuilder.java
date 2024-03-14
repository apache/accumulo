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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.AVAILABILITY;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_NONCE;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_REQUESTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.MERGED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.UNSPLITTABLE;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;

public class TabletMetadataBuilder implements Ample.TabletUpdates<TabletMetadataBuilder> {

  public static class InternalBuilder extends TabletMutatorBase<InternalBuilder> {
    protected InternalBuilder(KeyExtent extent) {
      super(extent);
    }

    @Override
    public Mutation getMutation() {
      return super.getMutation();
    }
  }

  private final InternalBuilder internalBuilder;
  EnumSet<TabletMetadata.ColumnType> fetched;

  protected TabletMetadataBuilder(KeyExtent extent) {
    internalBuilder = new InternalBuilder(extent);
    fetched = EnumSet.noneOf(TabletMetadata.ColumnType.class);
    putPrevEndRow(extent.prevEndRow());
  }

  @Override
  public TabletMetadataBuilder putPrevEndRow(Text per) {
    fetched.add(PREV_ROW);
    internalBuilder.putPrevEndRow(per);
    return this;
  }

  @Override
  public TabletMetadataBuilder putFile(ReferencedTabletFile path, DataFileValue dfv) {
    fetched.add(FILES);
    internalBuilder.putFile(path, dfv);
    return this;
  }

  @Override
  public TabletMetadataBuilder putFile(StoredTabletFile path, DataFileValue dfv) {
    fetched.add(FILES);
    internalBuilder.putFile(path, dfv);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteFile(StoredTabletFile path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putScan(StoredTabletFile path) {
    fetched.add(SCANS);
    internalBuilder.putScan(path);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteScan(StoredTabletFile path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putFlushId(long flushId) {
    fetched.add(FLUSH_ID);
    internalBuilder.putFlushId(flushId);
    return this;
  }

  @Override
  public TabletMetadataBuilder putFlushNonce(long flushNonce) {
    fetched.add(FLUSH_NONCE);
    internalBuilder.putFlushId(flushNonce);
    return this;
  }

  @Override
  public TabletMetadataBuilder putLocation(TabletMetadata.Location location) {
    fetched.add(LOCATION);
    internalBuilder.putLocation(location);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteLocation(TabletMetadata.Location location) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putZooLock(String zookeeperRoot, ServiceLock zooLock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putDirName(String dirName) {
    fetched.add(DIR);
    internalBuilder.putDirName(dirName);
    return this;
  }

  @Override
  public TabletMetadataBuilder putWal(LogEntry logEntry) {
    fetched.add(LOGS);
    internalBuilder.putWal(logEntry);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteWal(LogEntry logEntry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putTime(MetadataTime time) {
    fetched.add(TIME);
    internalBuilder.putTime(time);
    return this;
  }

  @Override
  public TabletMetadataBuilder putBulkFile(ReferencedTabletFile bulkref, FateId fateId) {
    fetched.add(LOADED);
    internalBuilder.putBulkFile(bulkref, fateId);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteBulkFile(StoredTabletFile bulkref) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putSuspension(TServerInstance tserver, long suspensionTime) {
    fetched.add(SUSPEND);
    internalBuilder.putSuspension(tserver, suspensionTime);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteSuspension() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putExternalCompaction(ExternalCompactionId ecid,
      CompactionMetadata ecMeta) {
    fetched.add(ECOMP);
    internalBuilder.putExternalCompaction(ecid, ecMeta);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteExternalCompaction(ExternalCompactionId ecid) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putCompacted(FateId fateId) {
    fetched.add(COMPACTED);
    internalBuilder.putCompacted(fateId);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteCompacted(FateId fateId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putTabletAvailability(TabletAvailability tabletAvailability) {
    fetched.add(AVAILABILITY);
    internalBuilder.putTabletAvailability(tabletAvailability);
    return this;
  }

  @Override
  public TabletMetadataBuilder setHostingRequested() {
    fetched.add(HOSTING_REQUESTED);
    internalBuilder.setHostingRequested();
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteHostingRequested() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putOperation(TabletOperationId opId) {
    fetched.add(OPID);
    internalBuilder.putOperation(opId);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteOperation() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putSelectedFiles(SelectedFiles selectedFiles) {
    fetched.add(SELECTED);
    internalBuilder.putSelectedFiles(selectedFiles);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteSelectedFiles() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder deleteAll(Set<Key> keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder setMerged() {
    fetched.add(MERGED);
    internalBuilder.setMerged();
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteMerged() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder putUserCompactionRequested(FateId fateId) {
    fetched.add(USER_COMPACTION_REQUESTED);
    internalBuilder.putUserCompactionRequested(fateId);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteUserCompactionRequested(FateId fateId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TabletMetadataBuilder setUnSplittable(UnSplittableMetadata unSplittableMeta) {
    fetched.add(UNSPLITTABLE);
    internalBuilder.setUnSplittable(unSplittableMeta);
    return this;
  }

  @Override
  public TabletMetadataBuilder deleteUnSplittable() {
    throw new UnsupportedOperationException();
  }

  /**
   * @param extraFetched Anything that was put on the builder will automatically be added to the
   *        fetched set. However, for the case where something was not put and it needs to be
   *        fetched it can be passed here. For example to simulate a tablet w/o a location it, no
   *        location will be put and LOCATION would be passed in via this argument.
   */
  public TabletMetadata build(TabletMetadata.ColumnType... extraFetched) {
    var mutation = internalBuilder.getMutation();

    SortedMap<Key,Value> rowMap = new TreeMap<>();
    mutation.getUpdates().forEach(cu -> {
      Key k = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
          cu.getTimestamp());
      Value v = new Value(cu.getValue());
      rowMap.put(k, v);
    });

    fetched.addAll(Arrays.asList(extraFetched));

    return TabletMetadata.convertRow(rowMap.entrySet().iterator(), fetched, true, false);
  }

}
