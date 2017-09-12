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

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeChooser.VolumeChooserException;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PreferredVolumeChooserTest {

  private static final String[] ALL_OPTIONS = new String[] {"1", "2", "3"};

  private ServerConfigurationFactory confFactory;
  private TableConfiguration tableConf;
  private PreferredVolumeChooser chooser;
  private AccumuloConfiguration systemConf;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    confFactory = createStrictMock(ServerConfigurationFactory.class);

    chooser = new PreferredVolumeChooser() {
      @Override
      ServerConfigurationFactory loadConfFactory() {
        return confFactory;
      }
    };

    tableConf = createStrictMock(TableConfiguration.class);
    systemConf = createStrictMock(AccumuloConfiguration.class);
    expect(confFactory.getTableConfiguration(anyObject())).andReturn(tableConf).anyTimes();
    expect(confFactory.getSystemConfiguration()).andReturn(systemConf).anyTimes();
  }

  @After
  public void after() throws Exception {
    verify(confFactory, tableConf, systemConf);
  }

  private String[] chooseForTable() {
    VolumeChooserEnvironment env = new VolumeChooserEnvironment(Table.ID.of("testTable"));
    return chooser.getPreferredVolumes(env, ALL_OPTIONS);
  }

  private String[] choose(ChooserScope scope) {
    VolumeChooserEnvironment env = new VolumeChooserEnvironment(scope);
    return chooser.getPreferredVolumes(env, ALL_OPTIONS);
  }

  @Test
  public void testInitScopeSelectsRandomlyFromAll() throws Exception {
    replay(confFactory, tableConf, systemConf);
    String[] volumes = choose(ChooserScope.INIT);
    assertSame(ALL_OPTIONS, volumes);
  }

  @Test
  public void testTableScopeUsingTableProperty() throws Exception {
    expect(tableConf.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn("2,1");
    replay(confFactory, tableConf, systemConf);

    String[] volumes = chooseForTable();
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"1", "2"}, volumes);
  }

  @Test
  public void testTableScopeUsingDefaultScopeProperty() throws Exception {
    expect(tableConf.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn(null).once();
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn("3,2").once();
    replay(confFactory, tableConf, systemConf);

    String[] volumes = chooseForTable();
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"2", "3"}, volumes);
  }

  @Test
  public void testTableScopeWithNoConfig() throws Exception {
    expect(tableConf.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn(null).once();
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn(null).once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    chooseForTable();
    fail("should not reach");
  }

  @Test
  public void testTableScopeWithEmptySet() throws Exception {
    expect(tableConf.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn(",").once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    chooseForTable();
    fail("should not reach");
  }

  @Test
  public void testTableScopeWithUnrecognizedVolumes() throws Exception {
    expect(tableConf.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn(null).once();
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn("4").once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    chooseForTable();
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeUsingLoggerProperty() throws Exception {
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn("2,1").once();
    replay(confFactory, tableConf, systemConf);

    String[] volumes = choose(ChooserScope.LOGGER);
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"1", "2"}, volumes);
  }

  @Test
  public void testLoggerScopeUsingDefaultProperty() throws Exception {
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(null).once();
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn("3,2").once();
    replay(confFactory, tableConf, systemConf);

    String[] volumes = choose(ChooserScope.LOGGER);
    Arrays.sort(volumes);
    assertArrayEquals(new String[] {"2", "3"}, volumes);
  }

  @Test
  public void testLoggerScopeWithNoConfig() throws Exception {
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(null).once();
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn(null).once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    choose(ChooserScope.LOGGER);
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeWithEmptySet() throws Exception {
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(",").once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    choose(ChooserScope.LOGGER);
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeWithUnrecognizedVolumes() throws Exception {
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(null).once();
    expect(systemConf.get(PreferredVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn("4").once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    choose(ChooserScope.LOGGER);
    fail("should not reach");
  }

}
