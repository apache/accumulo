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
package org.apache.accumulo.tserver.tablet;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.easymock.EasyMock;
import org.junit.Test;

public class TabletTest {

  @Test
  public void correctValuesSetForProperties() {
    TableConfiguration tableConf = EasyMock.createMock(TableConfiguration.class);
    CompactionPlan plan = EasyMock.createMock(CompactionPlan.class);
    WriteParameters writeParams = EasyMock.createMock(WriteParameters.class);
    plan.writeParameters = writeParams;

    long hdfsBlockSize = 10000L, blockSize = 5000L, indexBlockSize = 500L;
    int replication = 5;
    String compressType = "snappy";

    EasyMock.expect(tableConf.iterator()).andReturn(Collections.emptyIterator());
    EasyMock.expect(writeParams.getHdfsBlockSize()).andReturn(hdfsBlockSize).times(2);
    EasyMock.expect(writeParams.getBlockSize()).andReturn(blockSize).times(2);
    EasyMock.expect(writeParams.getIndexBlockSize()).andReturn(indexBlockSize).times(2);
    EasyMock.expect(writeParams.getCompressType()).andReturn(compressType).times(2);
    EasyMock.expect(writeParams.getReplication()).andReturn(replication).times(2);

    EasyMock.replay(tableConf, plan, writeParams);

    AccumuloConfiguration aConf = Tablet.createCompactionConfiguration(tableConf, plan);

    EasyMock.verify(tableConf, plan, writeParams);

    assertEquals(hdfsBlockSize, Long.parseLong(aConf.get(Property.TABLE_FILE_BLOCK_SIZE)));
    assertEquals(blockSize, Long.parseLong(aConf.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE)));
    assertEquals(indexBlockSize,
        Long.parseLong(aConf.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX)));
    assertEquals(compressType, aConf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
    assertEquals(replication, Integer.parseInt(aConf.get(Property.TABLE_FILE_REPLICATION)));
  }
}
