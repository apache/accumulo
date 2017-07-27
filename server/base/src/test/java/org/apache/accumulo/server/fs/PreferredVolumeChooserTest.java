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
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

public class PreferredVolumeChooserTest {
  private static final int REQUIRED_NUMBER_TRIES = 20; // times to call choose for likely exercising of each preferred volume
  private static final String[] ALL_OPTIONS = new String[] {"1", "2", "3"};
  private ServerConfigurationFactory mockedServerConfigurationFactory;
  private TableConfiguration mockedTableConfiguration;
  private PreferredVolumeChooser preferredVolumeChooser;
  private AccumuloConfiguration mockedAccumuloConfiguration;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    preferredVolumeChooser = new PreferredVolumeChooser();

    mockedServerConfigurationFactory = EasyMock.createMock(ServerConfigurationFactory.class);
    Field field = preferredVolumeChooser.getClass().getDeclaredField("serverConfs");
    field.setAccessible(true);
    field.set(preferredVolumeChooser, mockedServerConfigurationFactory);

    mockedTableConfiguration = EasyMock.createMock(TableConfiguration.class);
    mockedAccumuloConfiguration = EasyMock.createMock(AccumuloConfiguration.class);
  }

  private void configureDefaultVolumes(String configuredVolumes) {
    EasyMock.expect(mockedServerConfigurationFactory.getSystemConfiguration()).andReturn(mockedAccumuloConfiguration).anyTimes();
    EasyMock.expect(mockedAccumuloConfiguration.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn(configuredVolumes).anyTimes();
  }

  private void configureTableVolumes(String configuredVolumes) {
    EasyMock.expect(mockedServerConfigurationFactory.getTableConfiguration(EasyMock.<Table.ID> anyObject())).andReturn(mockedTableConfiguration).anyTimes();
    EasyMock.expect(mockedTableConfiguration.get(PreferredVolumeChooser.TABLE_PREFERRED_VOLUMES)).andReturn(configuredVolumes).anyTimes();
  }

  private void configureScopedVolumes(String configuredVolumes, String scope) {
    EasyMock.expect(mockedServerConfigurationFactory.getSystemConfiguration()).andReturn(mockedAccumuloConfiguration).anyTimes();
    EasyMock.expect(mockedAccumuloConfiguration.get(PreferredVolumeChooser.SCOPED_PREFERRED_VOLUMES(scope))).andReturn(configuredVolumes).anyTimes();
  }

  private void configureDefaultContextVolumes(String configuredVolumes) {
    EasyMock.expect(mockedAccumuloConfiguration.get(PreferredVolumeChooser.DEFAULT_SCOPED_PREFERRED_VOLUMES)).andReturn(configuredVolumes).anyTimes();
  }

  private void configureContextVolumes(String configuredVolumes) {
    EasyMock.expect(mockedAccumuloConfiguration.get(PreferredVolumeChooser.SCOPED_PREFERRED_VOLUMES("logger"))).andReturn(configuredVolumes).anyTimes();
  }

  private Set<String> chooseRepeatedlyForTable() {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Optional.of(Table.ID.of("h")));
    Set<String> results = new HashSet<>();
    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(preferredVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }
    return results;
  }

  private Set<String> chooseRepeatedlyForContext() {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Optional.empty());
    volumeChooserEnvironment.setScope("logger");
    Set<String> results = new HashSet<>();

    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(preferredVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }
    return results;
  }

  @Test
  public void testEmptyEnvUsesRandomChooser() throws Exception {
    VolumeChooserEnvironment volumeChooserEnvironment = new VolumeChooserEnvironment(Optional.empty());
    Set<String> results = new HashSet<>();
    for (int i = 0; i < REQUIRED_NUMBER_TRIES; i++) {
      results.add(preferredVolumeChooser.choose(volumeChooserEnvironment, ALL_OPTIONS));
    }

    Assert.assertEquals(Sets.newHashSet(Arrays.asList(ALL_OPTIONS)), results);
  }

  @Test
  public void testTableConfig() throws Exception {
    configureDefaultVolumes("1,3");
    configureTableVolumes("1,2");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForTable();

    EasyMock.verify(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("1", "2")), results);
  }

  @Test
  public void testTableMisconfigured() throws Exception {
    configureDefaultVolumes("1,3");
    configureTableVolumes("4");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableMissing() throws Exception {
    configureDefaultVolumes("1,3");
    configureTableVolumes(null);

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableEmptyConfig() throws Exception {
    configureDefaultVolumes("1,3");
    configureTableVolumes("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableMisconfiguredAndDefaultEmpty() throws Exception {
    configureDefaultVolumes("");
    configureTableVolumes("4");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testTableAndDefaultEmpty() throws Exception {
    configureDefaultVolumes("");
    configureTableVolumes("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedTableConfiguration, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForTable();
  }

  @Test
  public void testContextConfig() throws Exception {
    configureDefaultVolumes("1,3");
    configureContextVolumes("1,2");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    Set<String> results = chooseRepeatedlyForContext();

    EasyMock.verify(mockedServerConfigurationFactory, mockedAccumuloConfiguration);
    Assert.assertEquals(Sets.newHashSet(Arrays.asList("1", "2")), results);
  }

  @Test
  public void testContextMisconfigured() throws Exception {
    configureDefaultVolumes("1,3");
    configureContextVolumes("4");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForContext();
  }

  @Test
  public void testContextMissing() throws Exception {
    configureDefaultVolumes("1,3");
    configureContextVolumes(null);
    configureDefaultContextVolumes(null);

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForContext();
  }

  @Test
  public void testContextMisconfiguredAndDefaultEmpty() throws Exception {
    configureDefaultVolumes("");
    configureContextVolumes("4");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForContext();
  }

  @Test
  public void testContextAndDefaultBothEmpty() throws Exception {
    this.configureDefaultVolumes("");
    configureContextVolumes("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForContext();
  }

  @Test
  public void testContextEmptyConfig() throws Exception {
    configureDefaultVolumes("1,3");
    configureContextVolumes("");

    EasyMock.replay(mockedServerConfigurationFactory, mockedAccumuloConfiguration);

    thrown.expect(RuntimeException.class);
    chooseRepeatedlyForContext();
  }
}
