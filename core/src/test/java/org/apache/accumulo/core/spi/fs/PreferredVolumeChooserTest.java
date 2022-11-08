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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PreferredVolumeChooserTest {

  private static final String TABLE_CUSTOM_SUFFIX = "volume.preferred";

  private static final String getCustomPropertySuffix(Scope scope) {
    return "volume.preferred." + scope.name().toLowerCase();
  }

  private static final Set<String> ALL_OPTIONS = Set.of("1", "2", "3");

  private ServiceEnvironment serviceEnv;
  private Configuration tableConf;
  private Configuration systemConf;
  private PreferredVolumeChooser chooser;

  @BeforeEach
  public void before() {
    serviceEnv = createStrictMock(ServiceEnvironment.class);

    chooser = new PreferredVolumeChooser();

    tableConf = createStrictMock(Configuration.class);
    systemConf = createStrictMock(Configuration.class);
    expect(serviceEnv.getConfiguration(anyObject())).andReturn(tableConf).anyTimes();
    expect(serviceEnv.getConfiguration()).andReturn(systemConf).anyTimes();
  }

  @AfterEach
  public void after() {
    verify(serviceEnv, tableConf, systemConf);
  }

  private Set<String> chooseForTable() {
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
        return Scope.TABLE;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return serviceEnv;
      }

    };
    return chooser.getPreferredVolumes(env, ALL_OPTIONS);
  }

  private Set<String> choose(Scope scope) {
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
    return chooser.getPreferredVolumes(env, ALL_OPTIONS);
  }

  @Test
  public void testTableScopeUsingTableProperty() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn("2,1");
    replay(serviceEnv, tableConf, systemConf);
    assertEquals(Set.of("1", "2"), chooseForTable());
  }

  @Test
  public void testTableScopeUsingDefaultScopeProperty() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn("3,2").once();
    replay(serviceEnv, tableConf, systemConf);
    assertEquals(Set.of("2", "3"), chooseForTable());
  }

  @Test
  public void testTableScopeWithNoConfig() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn(null).once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, this::chooseForTable);
  }

  @Test
  public void testTableScopeWithEmptySet() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(",").once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, this::chooseForTable);
  }

  @Test
  public void testTableScopeWithUnrecognizedVolumes() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn("4").once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, this::chooseForTable);
  }

  @Test
  public void testLoggerScopeUsingLoggerProperty() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn("2,1").once();
    replay(serviceEnv, tableConf, systemConf);
    assertEquals(Set.of("1", "2"), choose(Scope.LOGGER));
  }

  @Test
  public void testLoggerScopeUsingDefaultProperty() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn("3,2").once();
    replay(serviceEnv, tableConf, systemConf);
    assertEquals(Set.of("2", "3"), choose(Scope.LOGGER));
  }

  @Test
  public void testLoggerScopeWithNoConfig() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn(null).once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, () -> choose(Scope.LOGGER));
  }

  @Test
  public void testLoggerScopeWithEmptySet() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(",").once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, () -> choose(Scope.LOGGER));
  }

  @Test
  public void testLoggerScopeWithUnrecognizedVolumes() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.LOGGER))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn("4").once();
    replay(serviceEnv, tableConf, systemConf);

    assertThrows(RuntimeException.class, () -> choose(Scope.LOGGER));
  }

  @Test
  public void testInitScopeUsingInitProperty() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.INIT))).andReturn("2,1").once();
    replay(serviceEnv, tableConf, systemConf);
    assertEquals(Set.of("1", "2"), choose(Scope.INIT));
  }

  @Test
  public void testInitScopeUsingDefaultProperty() {
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.INIT))).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(Scope.DEFAULT))).andReturn("3,2").once();
    replay(serviceEnv, tableConf, systemConf);
    assertEquals(Set.of("2", "3"), choose(Scope.INIT));
  }

}
