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
package org.apache.accumulo.tserver;

import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.tserver.Tablet.DatafileManager;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

/**
 *
 */
public class TabletTest {

  @Test
  public void correctValuesSetForProperties() {
    TableConfiguration tableConf = EasyMock.createMock(TableConfiguration.class);
    CompactionPlan plan = EasyMock.createMock(CompactionPlan.class);
    WriteParameters writeParams = EasyMock.createMock(WriteParameters.class);
    plan.writeParameters = writeParams;
    DatafileManager dfm = EasyMock.createMock(DatafileManager.class);
    TabletTime time = EasyMock.createMock(TabletTime.class);

    Tablet tablet = new Tablet(time, "", 0, new Path("/foo"), dfm);

    long hdfsBlockSize = 10000l, blockSize = 5000l, indexBlockSize = 500l;
    int replication = 5;
    String compressType = "snappy";

    EasyMock.expect(tableConf.iterator()).andReturn(Iterators.<Entry<String,String>> emptyIterator());
    EasyMock.expect(writeParams.getHdfsBlockSize()).andReturn(hdfsBlockSize).times(2);
    EasyMock.expect(writeParams.getBlockSize()).andReturn(blockSize).times(2);
    EasyMock.expect(writeParams.getIndexBlockSize()).andReturn(indexBlockSize).times(2);
    EasyMock.expect(writeParams.getCompressType()).andReturn(compressType).times(2);
    EasyMock.expect(writeParams.getReplication()).andReturn(replication).times(2);

    EasyMock.replay(tableConf, plan, writeParams);

    AccumuloConfiguration aConf = tablet.createTableConfiguration(tableConf, plan);

    EasyMock.verify(tableConf, plan, writeParams);

    Assert.assertEquals(hdfsBlockSize, Long.valueOf(aConf.get(Property.TABLE_FILE_BLOCK_SIZE)).longValue());
    Assert.assertEquals(blockSize, Long.valueOf(aConf.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE)).longValue());
    Assert.assertEquals(indexBlockSize, Long.valueOf(aConf.get(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX)).longValue());
    Assert.assertEquals(compressType, aConf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
    Assert.assertEquals(replication, Integer.valueOf(aConf.get(Property.TABLE_FILE_REPLICATION)).intValue());
  }

}
