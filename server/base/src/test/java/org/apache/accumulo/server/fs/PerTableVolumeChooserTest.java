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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

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

public class PerTableVolumeChooserTest {

  private ServerConfigurationFactory confFactory;
  private TableConfiguration tableConf;
  private PerTableVolumeChooser chooser;
  private AccumuloConfiguration systemConf;

  public static class MockChooser1 extends RandomVolumeChooser {}

  public static class MockChooser2 extends RandomVolumeChooser {}

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    confFactory = createStrictMock(ServerConfigurationFactory.class);

    chooser = new PerTableVolumeChooser() {
      @Override
      ServerConfigurationFactory loadConfFactory() {
        return confFactory;
      }

      @Override
      String getTableContext(TableConfiguration tableConf) {
        return null;
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

  private VolumeChooser getTableDelegate() {
    VolumeChooserEnvironment env = new VolumeChooserEnvironment(Table.ID.of("testTable"));
    return chooser.getDelegateChooser(env);
  }

  private VolumeChooser getDelegate(ChooserScope scope) {
    VolumeChooserEnvironment env = new VolumeChooserEnvironment(scope);
    return chooser.getDelegateChooser(env);
  }

  @Test
  public void testInitScopeSelectsRandomChooser() throws Exception {
    replay(confFactory, tableConf, systemConf);
    VolumeChooser delegate = getDelegate(ChooserScope.INIT);
    assertSame(RandomVolumeChooser.class, delegate.getClass());
  }

  @Test
  public void testTableScopeUsingTableProperty() throws Exception {
    expect(tableConf.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(MockChooser1.class.getName());
    replay(confFactory, tableConf, systemConf);

    VolumeChooser delegate = getTableDelegate();
    assertSame(MockChooser1.class, delegate.getClass());
  }

  @Test
  public void testTableScopeUsingDefaultScopeProperty() throws Exception {
    expect(tableConf.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(null).once();
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn(MockChooser2.class.getName()).once();
    replay(confFactory, tableConf, systemConf);

    VolumeChooser delegate = getTableDelegate();
    assertSame(MockChooser2.class, delegate.getClass());
  }

  @Test
  public void testTableScopeWithNoConfig() throws Exception {
    expect(tableConf.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(null).once();
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn(null).once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    getTableDelegate();
    fail("should not reach");
  }

  @Test
  public void testTableScopeWithBadDelegate() throws Exception {
    expect(tableConf.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(null).once();
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn("not a valid class name").once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    getTableDelegate();
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeUsingLoggerProperty() throws Exception {
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(MockChooser1.class.getName()).once();
    replay(confFactory, tableConf, systemConf);

    VolumeChooser delegate = getDelegate(ChooserScope.LOGGER);
    assertSame(MockChooser1.class, delegate.getClass());
  }

  @Test
  public void testLoggerScopeUsingDefaultProperty() throws Exception {
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(null).once();
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn(MockChooser2.class.getName()).once();
    replay(confFactory, tableConf, systemConf);

    VolumeChooser delegate = getDelegate(ChooserScope.LOGGER);
    assertSame(MockChooser2.class, delegate.getClass());
  }

  @Test
  public void testLoggerScopeWithNoConfig() throws Exception {
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(null).once();
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn(null).once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    getDelegate(ChooserScope.LOGGER);
    fail("should not reach");
  }

  @Test
  public void testLoggerScopeWithBadDelegate() throws Exception {
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.LOGGER))).andReturn(null).once();
    expect(systemConf.get(PerTableVolumeChooser.getPropertyNameForScope(ChooserScope.DEFAULT))).andReturn("not a valid class name").once();
    replay(confFactory, tableConf, systemConf);

    thrown.expect(VolumeChooserException.class);
    getDelegate(ChooserScope.LOGGER);
    fail("should not reach");
  }

}
