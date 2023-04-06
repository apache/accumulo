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
package org.apache.accumulo.core.spi.fs;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DelegatingChooserTest {

  private static final String TABLE_CUSTOM_SUFFIX = "volume.chooser";

  private static final String getCustomPropertySuffix(Scope scope) {
    return "volume.chooser." + scope.name().toLowerCase();
  }

  private ServiceEnvironment serviceEnv;
  private Configuration tableConf;
  private DelegatingChooser chooser;
  private Configuration systemConf;

  public static class MockChooser1 extends RandomVolumeChooser {}

  public static class MockChooser2 extends RandomVolumeChooser {}

  @BeforeEach
  public void before() {
    serviceEnv = createStrictMock(ServiceEnvironment.class);

    chooser = new DelegatingChooser();

    tableConf = createStrictMock(Configuration.class);
    systemConf = createStrictMock(Configuration.class);
    expect(serviceEnv.getConfiguration(anyObject())).andReturn(tableConf).anyTimes();
    expect(serviceEnv.getConfiguration()).andReturn(systemConf).anyTimes();
  }

  @AfterEach
  public void after() {
    verify(serviceEnv, tableConf, systemConf);
  }

  private VolumeChooser getTableDelegate() {
    VolumeChooserEnvironment env = new VolumeChooserEnvironment() {

      @Override
      public Text getEndRow() {
        return null;
      }

      @Override
      public Optional<TableId> getTable() {
        return Optional.of(TableId.of("testTable"));
      }

      @Override
      public Scope getChooserScope() {
        // TODO Auto-generated method stub
        return Scope.TABLE;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return serviceEnv;
      }
    };

    return chooser.getDelegateChooser(env);
  }

  private VolumeChooser getDelegate(Scope scope) {
    VolumeChooserEnvironment env = new VolumeChooserEnvironment() {

      @Override
      public Text getEndRow() {
        return null;
      }

      @Override
      public Optional<TableId> getTable() {
        return Optional.empty();
      }

      @Override
      public Scope getChooserScope() {
        return scope;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return serviceEnv;
      }

    };
    return chooser.getDelegateChooser(env);
  }

  @Test
  public void testTableScopeUsingTableProperty() throws Exception {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(MockChooser1.class.getName());
    expect(serviceEnv.instantiate(TableId.of("testTable"), MockChooser1.class.getName(),
        VolumeChooser.class)).andReturn(new MockChooser1());
    replay(serviceEnv, tableConf, systemConf);

    VolumeChooser delegate = getTableDelegate();
    assertSame(MockChooser1.class, delegate.getClass());
  }

  @Test
  public void testTableScopeUsingDefaultScopeProperty() throws Exception {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT)))
        .andReturn(MockChooser2.class.getName()).once();
    expect(serviceEnv.instantiate(TableId.of("testTable"), MockChooser2.class.getName(),
        VolumeChooser.class)).andReturn(new MockChooser2());
    replay(serviceEnv, tableConf, systemConf);

    VolumeChooser delegate = getTableDelegate();
    assertSame(MockChooser2.class, delegate.getClass());
  }

  @Test
  public void testTableScopeWithNoConfig() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn(null).once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, this::getTableDelegate);
  }

  @Test
  public void testTableScopeWithBadDelegate() throws Exception {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT)))
        .andReturn("not a valid class name").once();
    expect(serviceEnv.instantiate(TableId.of("testTable"), "not a valid class name",
        VolumeChooser.class)).andThrow(new RuntimeException());
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, this::getTableDelegate);
  }

  @Test
  public void testLoggerScopeUsingLoggerProperty() throws Exception {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER)))
        .andReturn(MockChooser1.class.getName()).once();
    expect(serviceEnv.instantiate(MockChooser1.class.getName(), VolumeChooser.class))
        .andReturn(new MockChooser1());
    replay(serviceEnv, tableConf, systemConf);

    VolumeChooser delegate = getDelegate(Scope.LOGGER);
    assertSame(MockChooser1.class, delegate.getClass());
  }

  @Test
  public void testLoggerScopeUsingDefaultProperty() throws Exception {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT)))
        .andReturn(MockChooser2.class.getName()).once();
    expect(serviceEnv.instantiate(MockChooser2.class.getName(), VolumeChooser.class))
        .andReturn(new MockChooser2());
    replay(serviceEnv, tableConf, systemConf);

    VolumeChooser delegate = getDelegate(Scope.LOGGER);
    assertSame(MockChooser2.class, delegate.getClass());
  }

  @Test
  public void testLoggerScopeWithNoConfig() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn(null).once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, () -> getDelegate(Scope.LOGGER));
  }

  @Test
  public void testLoggerScopeWithBadDelegate() throws Exception {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT)))
        .andReturn("not a valid class name").once();
    expect(serviceEnv.instantiate("not a valid class name", VolumeChooser.class))
        .andThrow(new RuntimeException());
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, () -> getDelegate(Scope.LOGGER));
  }

  @Test
  public void testInitScopeUsingInitProperty() throws Exception {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.INIT)))
        .andReturn(MockChooser1.class.getName()).once();
    expect(serviceEnv.instantiate(MockChooser1.class.getName(), VolumeChooser.class))
        .andReturn(new MockChooser1());
    replay(serviceEnv, tableConf, systemConf);

    VolumeChooser delegate = getDelegate(Scope.INIT);
    assertSame(MockChooser1.class, delegate.getClass());
  }

  @Test
  public void testInitScopeUsingDefaultProperty() throws Exception {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.INIT))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT)))
        .andReturn(MockChooser2.class.getName()).once();
    expect(serviceEnv.instantiate(MockChooser2.class.getName(), VolumeChooser.class))
        .andReturn(new MockChooser2());
    replay(serviceEnv, tableConf, systemConf);

    VolumeChooser delegate = getDelegate(Scope.INIT);
    assertSame(MockChooser2.class, delegate.getClass());
  }

}
