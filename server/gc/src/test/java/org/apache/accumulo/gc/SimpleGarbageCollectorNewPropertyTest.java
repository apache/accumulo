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
package org.apache.accumulo.gc;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.partialMockBuilder;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.gc.FileJanitor.SendFilesToTrash;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class SimpleGarbageCollectorNewPropertyTest extends SimpleGarbageCollectorTest {

  @Override
  protected ConfigurationCopy createSystemConfig() {
    Map<String,String> conf = new HashMap<>();
    conf.put(Property.INSTANCE_RPC_SASL_ENABLED.getKey(), "false");
    conf.put(Property.GC_CYCLE_START.getKey(), "1");
    conf.put(Property.GC_CYCLE_DELAY.getKey(), "20");
    conf.put(Property.GC_DELETE_THREADS.getKey(), "2");
    conf.put(Property.GC_USE_TRASH.getKey(), "true");

    return new ConfigurationCopy(conf);
  }

  @Test
  public void testMoveToTrash_NotUsingTrash_importsOnlyEnabled() throws Exception {
    systemConfig.set(Property.GC_USE_TRASH.getKey(), "bulk_imports_only");

    gc = partialMockBuilder(SimpleGarbageCollector.class).addMockedMethod("getContext")
        .addMockedMethod("getFileJanitor").createMock();
    FileJanitor janitor = new FileJanitor(context);
    expect(gc.getContext()).andReturn(context).anyTimes();
    expect(gc.getFileJanitor()).andReturn(janitor).anyTimes();
    replay(gc);

    assertEquals(SendFilesToTrash.IMPORTS_ONLY, gc.getFileJanitor().isUsingTrash());
    Path iFilePath = new Path("I0000070.rf");
    Path iFileWithFullPath =
        new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/I0000070.rf");
    Path notIFilePath = new Path("F0000070.rf");
    Path notIFileWithFullPath =
        new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf");
    expect(volMgr.moveToTrash(iFilePath)).andReturn(true).times(1);
    expect(volMgr.moveToTrash(iFileWithFullPath)).andReturn(true).times(1);
    replay(volMgr);

    assertTrue(gc.getFileJanitor().moveToTrash(iFilePath));
    assertTrue(gc.getFileJanitor().moveToTrash(iFileWithFullPath));
    assertFalse(gc.getFileJanitor().moveToTrash(notIFilePath));
    assertFalse(gc.getFileJanitor().moveToTrash(notIFileWithFullPath));

    verify(volMgr);
  }

}
