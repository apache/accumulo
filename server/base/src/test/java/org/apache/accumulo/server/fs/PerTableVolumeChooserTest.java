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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeChooser.VolumeChooserException;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

public class PerTableVolumeChooserTest {
  private static final int REQUIRED_NUMBER_TRIES = 20; // times to call choose for likely exercising of each preferred volume
  private static final String[] ALL_OPTIONS = new String[] {"1", "2", "3"};
  public static final String INVALID_CHOOSER_CLASSNAME = "MysteriousVolumeChooser";
  private ServerConfigurationFactory mockedServerConfigurationFactory;
  private TableConfiguration mockedTableConfiguration;
  private PerTableVolumeChooser perTableVolumeChooser;
  private AccumuloConfiguration mockedAccumuloConfiguration;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    perTableVolumeChooser = new PerTableVolumeChooser();

    mockedServerConfigurationFactory = EasyMock.createMock(ServerConfigurationFactory.class);
    Field field = perTableVolumeChooser.getClass().getDeclaredField("lazyConfFactory");
    field.setAccessible(true);
    field.set(perTableVolumeChooser, mockedServerConfigurationFactory);

    mockedTableConfiguration = EasyMock.createMock(TableConfiguration.class);
    mockedAccumuloConfiguration = EasyMock.createMock(AccumuloConfiguration.class);
    EasyMock.expect(mockedServerConfigurationFactory.getTableConfiguration(EasyMock.<Table.ID> anyObject())).andReturn(mockedTableConfiguration).anyTimes();
    EasyMock.expect(mockedServerConfigurationFactory.getSystemConfiguration()).andReturn(mockedAccumuloConfiguration).anyTimes();
    EasyMock.expect(mockedTableConfiguration.get(Property.TABLE_CLASSPATH)).andReturn(null).anyTimes();
  }

  private IExpectationSetters<String> expectDefaultScope(String className) {
    return expectScope(ChooserScope.DEFAULT, className);
  }

  private IExpectationSetters<String> expectLoggerScope(String className) {
    return expectScope(ChooserScope.LOGGER, className);
  }

  private IExpectationSetters<String> expectScope(ChooserScope scope, String className) {
    return EasyMock.expect(mockedAccumuloConfiguration.get(PerTableVolumeChooser.getPropertyNameForScope(scope))).andReturn(className);
  }

  private IExpectationSetters<String> expectTableChooser(String className) {
    return EasyMock.expect(mockedTableConfiguration.get(PerTableVolumeChooser.TABLE_VOLUME_CHOOSER)).andReturn(className);
  }

  private Set<String> chooseRepeatedlyForTable() {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Table.ID.of("h"));
    Set<String> results = new HashSet<>();
    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(perTableVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }
    return results;
  }

  public static class VolumeChooserAlwaysOne extends VolumeChooserForFixedVolume {
    public VolumeChooserAlwaysOne() {
      super("1");
    }
  }

  public static class VolumeChooserAlwaysTwo extends VolumeChooserForFixedVolume {
    public VolumeChooserAlwaysTwo() {
      super("2");
    }
  }

  public static class VolumeChooserAlwaysThree extends VolumeChooserForFixedVolume {
    public VolumeChooserAlwaysThree() {
      super("3");
    }
  }

  public static class VolumeChooserForFixedVolume implements VolumeChooser {
    private final String onlyValidOption;

    public VolumeChooserForFixedVolume(String fixedVolume) {
      onlyValidOption = fixedVolume;
    }

    @Override
    public String choose(VolumeChooserEnvironment env, String[] options) {
      for (String option : options) {
        if (onlyValidOption.equals(option)) {
          return onlyValidOption;
        }
      }
      return null;
    }
  }

  private Set<String> chooseRepeatedlyForLogger() {
    return chooseRepeatedlyForScope(ChooserScope.LOGGER);
  }

  private Set<String> chooseRepeatedlyForScope(ChooserScope scope) {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(scope);
    Set<String> results = new HashSet<>();

    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(perTableVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }
    return results;
  }

  @Test
  public void testInitScope() throws Exception {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(ChooserScope.INIT);

    Set<String> results = new HashSet<>();
    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(perTableVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }

    Assert.assertEquals(Sets.newHashSet(Arrays.asList(ALL_OPTIONS)), results);
  }

  @Test
  public void testTableConfig() throws Exception {
    expectTableChooser(VolumeChooserAlwaysTwo.class.getName()).atLeastOnce();

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForTable();

    EasyMock.verify(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("2")), results);
  }

  @Test
  public void testTableMisconfigured() throws Exception {
    expectDefaultScope(VolumeChooserAlwaysOne.class.getName());
    expectTableChooser(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableMissing() throws Exception {
    expectDefaultScope(null);
    expectTableChooser(null);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableMisconfiguredAndDefaultEmpty() throws Exception {
    expectDefaultScope("");
    expectTableChooser(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableEmptyConfig() throws Exception {
    expectDefaultScope("");
    expectTableChooser("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableAndDefaultEmpty() throws Exception {
    expectDefaultScope("");
    expectTableChooser("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testScopeConfig() throws Exception {
    expectLoggerScope(VolumeChooserAlwaysOne.class.getName()).atLeastOnce();

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForLogger();

    EasyMock.verify(mockedServerConfigurationFactory, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("1")), results);
  }

  @Test
  public void testScopeMisconfigured() throws Exception {
    expectDefaultScope(VolumeChooserAlwaysThree.class.getName());
    expectLoggerScope(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForLogger();
  }

  @Test
  public void testScopeMissing() throws Exception {
    expectLoggerScope(null);
    expectDefaultScope(null);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForLogger();
  }

  @Test
  public void testScopeMisconfiguredAndDefaultEmpty() throws Exception {
    expectDefaultScope("");
    expectTableChooser("");
    expectLoggerScope(INVALID_CHOOSER_CLASSNAME);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForLogger();
  }

  @Test
  public void testScopeAndDefaultBothEmpty() throws Exception {
    expectDefaultScope("");
    expectLoggerScope("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(VolumeChooserException.class);
    chooseRepeatedlyForLogger();
  }

}
