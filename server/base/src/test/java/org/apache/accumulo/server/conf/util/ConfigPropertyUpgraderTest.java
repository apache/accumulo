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

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.junit.jupiter.api.Test;

class ConfigPropertyUpgraderTest {

  @Test
  void upgradeSysProps() {

    InstanceId iid = InstanceId.of(UUID.randomUUID());

    ConfigPropertyUpgrader upgrader = new ConfigPropertyUpgrader();
    ConfigTransformer transformer = createMock(ConfigTransformer.class);
    expect(transformer.transform(SystemPropKey.of(iid), SystemPropKey.of(iid).getPath(), false))
        .andReturn(new VersionedProperties()).once();

    replay(transformer);
    upgrader.upgradeSysProps(iid, transformer);
    verify(transformer);
  }

  @Test
  void upgradeNamespaceProps() throws Exception {

    InstanceId iid = InstanceId.of(UUID.randomUUID());

    ConfigPropertyUpgrader upgrader = new ConfigPropertyUpgrader();

    ConfigTransformer transformer = createMock(ConfigTransformer.class);
    String nsRoot = ZooUtil.getRoot(iid) + Constants.ZNAMESPACES;
    expect(transformer.transform(NamespacePropKey.of(iid, NamespaceId.of("a")),
        nsRoot + "/a" + Constants.ZCONF_LEGACY, true)).andReturn(new VersionedProperties()).once();
    expect(transformer.transform(NamespacePropKey.of(iid, NamespaceId.of("b")),
        nsRoot + "/b" + Constants.ZCONF_LEGACY, true)).andReturn(new VersionedProperties()).once();

    ZooReaderWriter zrw = createMock(ZooReaderWriter.class);
    expect(zrw.getChildren(anyString())).andReturn(List.of("a", "b")).once();

    replay(transformer, zrw);
    upgrader.upgradeNamespaceProps(iid, zrw, transformer);
    verify(transformer, zrw);
  }

  @Test
  void upgradeTableProps() throws Exception {

    InstanceId iid = InstanceId.of(UUID.randomUUID());

    ConfigPropertyUpgrader upgrader = new ConfigPropertyUpgrader();

    ConfigTransformer transformer = createMock(ConfigTransformer.class);
    String nsRoot = ZooUtil.getRoot(iid) + Constants.ZTABLES;
    expect(transformer.transform(TablePropKey.of(iid, TableId.of("a")),
        nsRoot + "/a" + Constants.ZCONF_LEGACY, true)).andReturn(new VersionedProperties()).once();
    expect(transformer.transform(TablePropKey.of(iid, TableId.of("b")),
        nsRoot + "/b" + Constants.ZCONF_LEGACY, true)).andReturn(new VersionedProperties()).once();

    ZooReaderWriter zrw = createMock(ZooReaderWriter.class);
    expect(zrw.getChildren(anyString())).andReturn(List.of("a", "b")).once();

    replay(transformer, zrw);
    upgrader.upgradeTableProps(iid, zrw, transformer);
    verify(transformer, zrw);
  }
}
