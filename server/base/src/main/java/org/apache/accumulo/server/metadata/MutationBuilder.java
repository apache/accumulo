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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.metadata.MetadataMutator.TabletMutator;
import org.apache.hadoop.io.Text;

public abstract class MutationBuilder implements TabletMutator {

  private final ServerContext context;
  private final KeyExtent extent;
  private final Mutation mutation;

  protected MutationBuilder(ServerContext ctx, KeyExtent extent) {
    this.extent = extent;
    this.context = ctx;
    mutation = new Mutation(extent.getMetadataEntry());
  }

  @Override
  public TabletMutator putPrevEndRow(Text per) {
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(mutation,
        KeyExtent.encodePrevEndRow(extent.getPrevEndRow()));
    return this;
  }

  @Override
  public TabletMutator putDir(String dir) {
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mutation,
        new Value(dir.getBytes(UTF_8)));
    return this;
  }

  @Override
  public TabletMutator putFile(FileRef path, DataFileValue dfv) {
    mutation.put(DataFileColumnFamily.NAME, path.meta(), new Value(dfv.encode()));
    return this;
  }

  @Override
  public TabletMutator putScan(FileRef path) {
    mutation.put(ScanFileColumnFamily.NAME, path.meta(), new Value(new byte[0]));
    return this;
  }

  @Override
  public TabletMutator deleteFile(FileRef path) {
    mutation.putDelete(DataFileColumnFamily.NAME, path.meta());
    return this;
  }

  @Override
  public TabletMutator putCompactionId(long compactionId) {
    TabletsSection.ServerColumnFamily.COMPACT_COLUMN.put(mutation,
        new Value(("" + compactionId).getBytes()));
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
  public TabletMutator putLocation(HostAndPort location, String session, LocationType type) {
    mutation.put(getLocationFamily(type), session, location.toString());
    return this;
  }

  @Override
  public TabletMutator deleteLocation(String session, LocationType type) {
    mutation.putDelete(getLocationFamily(type), session);
    return this;
  }

  @Override
  public TabletMutator putLocation(TServerInstance tsi, LocationType type) {
    mutation.put(getLocationFamily(type), tsi.getSession(), tsi.hostPort());
    return this;
  }

  @Override
  public TabletMutator deleteLocation(TServerInstance tsi, LocationType type) {
    mutation.putDelete(getLocationFamily(type), tsi.getSession());
    return this;
  }

  @Override
  public TabletMutator putZooLock(ZooLock zooLock) {
    TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(mutation,
        new Value(zooLock.getLockID().serialize(context.getZooKeeperRoot() + "/").getBytes(UTF_8)));
    return this;
  }

  @Override
  public TabletMutator putWal(LogEntry logEntry) {
    mutation.put(logEntry.getColumnFamily(), logEntry.getColumnQualifier(), logEntry.getValue());
    return this;
  }

  @Override
  public TabletMutator deleteWal(LogEntry logEntry) {
    mutation.putDelete(logEntry.getColumnFamily(), logEntry.getColumnQualifier());
    return this;
  }

  @Override
  public TabletMutator deleteWal(String wal) {
    mutation.putDelete(MetadataSchema.TabletsSection.LogColumnFamily.STR_NAME, wal);
    return this;
  }

  protected Mutation getMutation() {
    return mutation;
  }
}
