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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZINSTANCES;
import static org.apache.accumulo.core.Constants.ZROOT;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZooPropUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(ZooPropUtilsTest.class);

  @Test
  public void fetchInstancesFromZk() throws Exception {

    String instAName = "INST_A";
    InstanceId instA = InstanceId.of(UUID.randomUUID());
    String instBName = "INST_B";
    InstanceId instB = InstanceId.of(UUID.randomUUID());

    ZooReader zooReader = createMock(ZooReader.class);
    String namePath = ZROOT + ZINSTANCES;
    expect(zooReader.getChildren(eq(namePath))).andReturn(List.of(instAName, instBName)).once();
    expect(zooReader.getData(eq(namePath + "/" + instAName)))
        .andReturn(instA.canonical().getBytes(UTF_8)).once();
    expect(zooReader.getData(eq(namePath + "/" + instBName)))
        .andReturn(instB.canonical().getBytes(UTF_8)).once();
    replay(zooReader);

    Map<String,InstanceId> instanceMap = ZooPropUtils.readInstancesFromZk(zooReader);

    LOG.trace("id map returned: {}", instanceMap);
    assertEquals(Map.of(instAName, instA, instBName, instB), instanceMap);
    verify(zooReader);
  }

}
