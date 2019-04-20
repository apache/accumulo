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
package org.apache.accumulo.server.fs;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.server.fs.VolumeChooser.VolumeChooserException;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PreferredVolumeChooserTest {

  private static final String TABLE_CUSTOM_SUFFIX = "volume.preferred";

  private static final String getCustomPropertySuffix(ChooserScope scope) {
    return "volume.preferred." + scope.name().toLowerCase();
  }

  private static final String[] ALL_OPTIONS = {"1", "2", "3"};

  private ServiceEnvironment serviceEnv;
  private Configuration tableConf;
  private Configuration systemConf;
  private PreferredVolumeChooser chooser;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() {
    serviceEnv = createStrictMock(ServiceEnvironment.class);

    chooser = new PreferredVolumeChooser();

    tableConf = createStrictMock(Configuration.class);
    systemConf = createStrictMock(Configuration.class);
    expect(serviceEnv.getConfiguration(anyObject())).andReturn(tableConf).anyTimes();
    expect(serviceEnv.getConfiguration()).andReturn(systemConf).anyTimes();
  }

  @After
  public void after() {
    verify(serviceEnv, tableConf, systemConf);
  }

  private String[] chooseForTable() {
    VolumeChooserEnvironment env =
        new VolumeChooserEnvironmentImpl(TableId.of("testTable"), null, null) {
          @Override
          public ServiceEnvironment getServiceEnv() {
            return serviceEnv;
          }
        };
    return chooser.getPreferredVolumes(env, ALL_OPTIONS);
  }

  private String[] choose(ChooserScope scope) {
    VolumeChooserEnvironment env = new VolumeChooserEnvironmentImpl(scope, null) {
      @Override
      public ServiceEnvironment getServiceEnv() {
        return serviceEnv;
      }
    };
    return chooser.getPreferredVolumes(env, ALL_OPTIONS);
  }

  @Test
  public void testInitScopeSelectsRandomlyFromAll() {
    replay(serviceEnv, tableConf, systemConf);
    String[] volumes = choose(ChooserScope.INIT);
    assertSame(ALL_OPTIONS, volumes);
  }

  @Test
  public void testTableScopeUsingTableProperty() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn("2,1");
    replay(serviceEnv, tableConf, systemConf);

    String[] volumes = chooseForTable();
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"1", "2"}, volumes);
  }

  @Test
  public void testTableScopeUsingDefaultScopeProperty() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.DEFAULT))).andReturn("3,2")
        .once();
    replay(serviceEnv, tableConf, systemConf);

    String[] volumes = chooseForTable();
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"2", "3"}, volumes);
  }

  @Test
  public void testTableScopeWithNoConfig() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.DEFAULT))).andReturn(null)
        .once();
    replay(serviceEnv, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    chooseForTable();
    fail("should not reach");
  }

  @Test
  public void testTableScopeWithEmptySet() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(",").once();
    replay(serviceEnv, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    chooseForTable();
    fail("should not reach");
  }

  @Test
  public void testTableScopeWithUnrecognizedVolumes() {
    expect(tableConf.getTableCustom(TABLE_CUSTOM_SUFFIX)).andReturn(null).once();
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.DEFAULT))).andReturn("4")
        .once();
    replay(serviceEnv, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    chooseForTable();
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeUsingLoggerProperty() {
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.LOGGER))).andReturn("2,1")
        .once();
    replay(serviceEnv, tableConf, systemConf);

    String[] volumes = choose(ChooserScope.LOGGER);
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"1", "2"}, volumes);
  }

  @Test
  public void testLoggerScopeUsingDefaultProperty() {
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.LOGGER))).andReturn(null)
        .once();
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.DEFAULT))).andReturn("3,2")
        .once();
    replay(serviceEnv, tableConf, systemConf);

    String[] volumes = choose(ChooserScope.LOGGER);
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"2", "3"}, volumes);
  }

  @Test
  public void testLoggerScopeWithNoConfig() {
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.LOGGER))).andReturn(null)
        .once();
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.DEFAULT))).andReturn(null)
        .once();
    replay(serviceEnv, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    choose(ChooserScope.LOGGER);
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeWithEmptySet() {
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.LOGGER))).andReturn(",")
        .once();
    replay(serviceEnv, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    choose(ChooserScope.LOGGER);
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeWithUnrecognizedVolumes() {
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.LOGGER))).andReturn(null)
        .once();
    expect(systemConf.getCustom(getCustomPropertySuffix(ChooserScope.DEFAULT))).andReturn("4")
        .once();
    replay(serviceEnv, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    choose(ChooserScope.LOGGER);
    fail("should not reach");
  }

}
