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
package org.apache.accumulo.manager.split;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class SplitterTest {

  @Test
  public void testShouldInspect() {
    ThreadPools threadPools = createNiceMock(ThreadPools.class);
    replay(threadPools);
    ServerContext context = createNiceMock(ServerContext.class);
    expect(context.threadPools()).andReturn(threadPools).anyTimes();
    replay(context);

    var splitter = new Splitter(context);

    KeyExtent ke1 = new KeyExtent(TableId.of("1"), new Text("m"), null);
    KeyExtent ke2 = new KeyExtent(TableId.of("1"), null, new Text("m"));

    Set<StoredTabletFile> files1 = Set.of(
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf"),
        new StoredTabletFile(
            "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf"));

    TabletMetadata tabletMeta1 = createMock(TabletMetadata.class);
    expect(tabletMeta1.getExtent()).andReturn(ke1).anyTimes();
    expect(tabletMeta1.getFiles()).andReturn(files1).anyTimes();
    replay(tabletMeta1);

    TabletMetadata tabletMeta2 = createMock(TabletMetadata.class);
    expect(tabletMeta2.getExtent()).andReturn(ke2).anyTimes();
    replay(tabletMeta2);

    assertTrue(splitter.isSplittable(tabletMeta1));
    assertTrue(splitter.isSplittable(tabletMeta2));

    splitter.addSplitStarting(ke1);

    assertFalse(splitter.isSplittable(tabletMeta1));
    assertTrue(splitter.isSplittable(tabletMeta2));

    splitter.removeSplitStarting(ke1);

    assertTrue(splitter.isSplittable(tabletMeta1));
    assertTrue(splitter.isSplittable(tabletMeta2));

    splitter.rememberUnsplittable(tabletMeta1);

    assertFalse(splitter.isSplittable(tabletMeta1));
    assertTrue(splitter.isSplittable(tabletMeta2));

    // when a tablets files change it should become a candidate for inspection
    Set<StoredTabletFile> files2 = Set.of(
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf"),
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf"),
        new StoredTabletFile(
            "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000073.rf"));
    TabletMetadata tabletMeta3 = createMock(TabletMetadata.class);
    expect(tabletMeta3.getExtent()).andReturn(ke1).anyTimes();
    expect(tabletMeta3.getFiles()).andReturn(files2).anyTimes();
    replay(tabletMeta3);

    assertTrue(splitter.isSplittable(tabletMeta3));
    assertTrue(splitter.isSplittable(tabletMeta2));
  }

}
