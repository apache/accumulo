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
package org.apache.accumulo.server.tablets;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class TabletNameGeneratorTest {

  @Test
  public void testGetNextDataFilenameForMajc() {

    String instanceId = UUID.randomUUID().toString();
    String baseUri = "hdfs://localhost:8000/accumulo/" + instanceId;
    TableId tid = TableId.of("1");
    KeyExtent ke1 = new KeyExtent(tid, new Text("b"), new Text("a"));
    String dirName = "t-000001";

    ConfigurationCopy conf = new ConfigurationCopy();

    TableConfiguration tableConf = EasyMock.createMock(TableConfiguration.class);
    EasyMock.expect(tableConf.get(Property.TABLE_FILE_TYPE)).andReturn("rf");

    VolumeManager vm = EasyMock.createMock(VolumeManager.class);
    EasyMock.expect(
        vm.choose(EasyMock.isA(VolumeChooserEnvironmentImpl.class), EasyMock.eq(Set.of(baseUri))))
        .andReturn(baseUri);

    UniqueNameAllocator allocator = EasyMock.createMock(UniqueNameAllocator.class);
    EasyMock.expect(allocator.getNextName()).andReturn("NextFileName");

    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(context.getInstanceID()).andReturn(InstanceId.of(instanceId)).anyTimes();
    EasyMock.expect(context.getConfiguration()).andReturn(conf);
    EasyMock.expect(context.getTableConfiguration(tid)).andReturn(tableConf);
    EasyMock.expect(context.getCaches()).andReturn(Caches.getInstance());
    EasyMock.expect(context.getVolumeManager()).andReturn(vm);
    EasyMock.expect(context.getBaseUris()).andReturn(Set.of(baseUri));
    EasyMock.expect(context.getUniqueNameAllocator()).andReturn(allocator);

    TabletMetadata tm1 = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm1.getExtent()).andReturn(ke1);
    EasyMock.expect(tm1.getDirName()).andReturn(dirName);

    Consumer<String> dirCreator = (dir) -> {};
    ExternalCompactionId ecid = ExternalCompactionId.generate(UUID.randomUUID());

    EasyMock.replay(tableConf, vm, allocator, context, tm1);

    ReferencedTabletFile rtf =
        TabletNameGenerator.getNextDataFilenameForMajc(false, context, tm1, dirCreator, ecid);
    assertEquals("ANextFileName.rf_tmp_" + ecid.canonical(), rtf.getFileName());

    EasyMock.verify(tableConf, vm, allocator, context, tm1);

  }
}
