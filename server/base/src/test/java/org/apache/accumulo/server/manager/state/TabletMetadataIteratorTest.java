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
package org.apache.accumulo.server.manager.state;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.VirtualMetadataColumns.MaintenanceRequired;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.VirtualMetadataColumns.MaintenanceRequired.Reasons;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletMetadataIteratorTest {

  private SortedMap<Key,Value> toRowMap(Mutation mutation) {
    SortedMap<Key,Value> rowMap = new TreeMap<>();
    mutation.getUpdates().forEach(cu -> {
      Key k = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
          cu.getTimestamp());
      Value v = new Value(cu.getValue());
      rowMap.put(k, v);
    });
    return rowMap;
  }

  private SortedMap<Key,Value> createMetadataEntryKV() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);

    COMPACT_COLUMN.put(mutation, new Value("5"));
    DIRECTORY_COLUMN.put(mutation, new Value("t-0001757"));
    FLUSH_COLUMN.put(mutation, new Value("6"));
    TIME_COLUMN.put(mutation, new Value("M123456789"));

    String bf1 = "hdfs://nn1/acc/tables/1/t-0001/bf1";
    String bf2 = "hdfs://nn1/acc/tables/1/t-0001/bf2";
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf1).put(FateTxId.formatTid(56));
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf2).put(FateTxId.formatTid(59));

    mutation.at().family(ClonedColumnFamily.NAME).qualifier("").put("OK");

    DataFileValue dfv1 = new DataFileValue(555, 23);
    StoredTabletFile tf1 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/df1.rf");
    StoredTabletFile tf2 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/df2.rf");
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf1.getMetaUpdateDelete())
        .put(dfv1.encode());
    DataFileValue dfv2 = new DataFileValue(234, 13);
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf2.getMetaUpdateDelete())
        .put(dfv2.encode());

    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    mutation.at().family(LastLocationColumnFamily.NAME).qualifier("s000").put("server2:8555");

    LogEntry le1 = new LogEntry(extent, 55, "lf1");
    mutation.at().family(le1.getColumnFamily()).qualifier(le1.getColumnQualifier())
        .timestamp(le1.timestamp).put(le1.getValue());
    LogEntry le2 = new LogEntry(extent, 57, "lf2");
    mutation.at().family(le2.getColumnFamily()).qualifier(le2.getColumnQualifier())
        .timestamp(le2.timestamp).put(le2.getValue());

    StoredTabletFile sf1 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/sf1.rf");
    StoredTabletFile sf2 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/sf2.rf");
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf1.getMetaUpdateDelete()).put("");
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf2.getMetaUpdateDelete()).put("");

    return toRowMap(mutation);

  }

  @Test
  public void testEncodeDecodeWithReasons() throws Exception {
    Set<Reasons> reasons = Set.of(Reasons.NEEDS_LOCATION_UPDATE, Reasons.NEEDS_SPLITTING);
    SortedMap<Key,Value> entries = createMetadataEntryKV();
    MaintenanceRequired.addReasons(entries, reasons);
    Key key = entries.firstKey();
    Value val = WholeRowIterator.encodeRow(new ArrayList<>(entries.keySet()),
        new ArrayList<>(entries.values()));
    SortedMap<Key,Value> decode = WholeRowIterator.decodeRow(key, val);
    assertEquals(entries, decode);
    TabletMetadata tm =
        TabletMetadataIterator.decode(Map.of(key, val).entrySet().iterator().next());
    assertEquals(reasons, tm.getMaintenanceReasons().orElseThrow());
  }

}
