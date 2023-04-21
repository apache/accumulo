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
package org.apache.accumulo.server.util;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ManagerMetadataUtilTest {

  private AccumuloConfiguration conf;
  private ClientContext context;
  private Ample ample;
  private Ample.TabletMutator tabletMutator;
  private TabletMetadata tabletMetadata;
  private final KeyExtent extent = new KeyExtent(MetadataTable.ID, null, null);
  private final TServerInstance server1 = new TServerInstance("127.0.0.1:10000", 0);
  private final Location last1 = Location.last(server1);
  private final TServerInstance server2 = new TServerInstance("127.0.0.2:10000", 1);
  private final Location last2 = Location.last(server2);

  @BeforeEach
  public void before() {
    conf = EasyMock.createMock(AccumuloConfiguration.class);
    EasyMock.expect(conf.get(Property.TSERV_LAST_LOCATION_MODE)).andReturn("assignment");
    context = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(conf).once();
    ample = EasyMock.createMock(Ample.class);
    tabletMutator = EasyMock.createMock(Ample.TabletMutator.class);
    tabletMetadata = EasyMock.createMock(TabletMetadata.class);
  }

  @AfterEach
  public void after() {
    EasyMock.verify(conf, context, ample, tabletMetadata, tabletMutator);
  }

  @Test
  public void testUpdateLastForAssignModeNullLastLocationSame() {
    // Expect a call to read last location from Ample as the provided last location
    // argument to updateLastForAssignmentMode will be missing
    EasyMock.expect(tabletMetadata.getLast()).andReturn(last1);
    EasyMock.expect(ample.readTablet(extent, TabletMetadata.ColumnType.LAST))
        .andReturn(tabletMetadata).once();

    EasyMock.replay(conf, context, ample, tabletMetadata, tabletMutator);

    // Pass in a null last location value. With this scenario
    // we have to read the last location using Ample. There should be
    // no call to tabletMutator.putLocation or tabletMutator.deleteLocation
    // as the previous last location read from Ample matches server 1
    ManagerMetadataUtil.updateLastForAssignmentMode(context, ample, tabletMutator, extent, server1,
        null);
  }

  @Test
  public void testUpdateLastForAssignModeNullLastLocationDifferent() {
    // Expect a call to read last location from Ample as the provided last location
    // argument to updateLastForAssignmentMode will be missing
    EasyMock.expect(tabletMetadata.getLast()).andReturn(last1);
    EasyMock.expect(ample.readTablet(extent, TabletMetadata.ColumnType.LAST))
        .andReturn(tabletMetadata).once();

    // Expect a delete of last1 as that is the previous and put location of last2 for server 2
    EasyMock.expect(tabletMutator.deleteLocation(last1)).andReturn(tabletMutator).once();
    EasyMock.expect(tabletMutator.putLocation(last2)).andReturn(tabletMutator).once();

    EasyMock.replay(conf, context, ample, tabletMetadata, tabletMutator);

    // Pass in a null last location value. With this scenario
    // we have to read the last location using Ample. There should be
    // a call to tabletMutator.putLocation for last2 and tabletMutator.deleteLocation
    // of last1 as the last location is being updated
    ManagerMetadataUtil.updateLastForAssignmentMode(context, ample, tabletMutator, extent, server2,
        null);
  }

  @Test
  public void testUpdateLastForAssignmentModeNullLastLocationUpdateReturnNull() {
    // Expect a call to read last location from Ample as the provided last location
    // argument to updateLastForAssignmentMode will be missing
    // Return null last location from Ample
    EasyMock.expect(tabletMetadata.getLast()).andReturn(null);
    EasyMock.expect(ample.readTablet(extent, TabletMetadata.ColumnType.LAST))
        .andReturn(tabletMetadata).once();

    // Expect a put of last1 as the previous value from Ample is also null
    EasyMock.expect(tabletMutator.putLocation(last1)).andReturn(tabletMutator).once();

    EasyMock.replay(conf, context, ample, tabletMetadata, tabletMutator);

    // Pass in a null last location value. With this scenario
    // we have to read the last location using Ample. There should be
    // a call to tabletMutator.putLocation of last 1 but the read from Ample
    // returns null so no deletion
    ManagerMetadataUtil.updateLastForAssignmentMode(context, ample, tabletMutator, extent, server1,
        null);
  }

  @Test
  public void testUpdateLastForAssignModeProvideLastLocationSame() {
    EasyMock.replay(conf, context, ample, tabletMetadata, tabletMutator);

    // Pass in a last location value that matches the new value of server 1
    // There should be no read from Ample as we provided a value as an argument
    // and no call to tabletMutator.putLocation or tabletMutator.deleteLocation
    // as the locations are equal
    ManagerMetadataUtil.updateLastForAssignmentMode(context, ample, tabletMutator, extent, server1,
        last1);
  }

  @Test
  public void testUpdateLastForAssignModeProvidedLastLocationDifferent() {
    // Expect a delete of last1 as we are providing that as the previous last location
    // which is different than server 2 location
    EasyMock.expect(tabletMutator.deleteLocation(last1)).andReturn(tabletMutator).once();
    EasyMock.expect(tabletMutator.putLocation(last2)).andReturn(tabletMutator).once();

    EasyMock.replay(conf, context, ample, tabletMetadata, tabletMutator);

    // Pass in last1 as the last location value.
    // There should be no read from Ample as we provided a value as an argument
    // There should be a call to tabletMutator.putLocation and tabletMutator.deleteLocation
    // as the last location is being updated as last1 does not match server 2
    ManagerMetadataUtil.updateLastForAssignmentMode(context, ample, tabletMutator, extent, server2,
        last1);
  }

}
