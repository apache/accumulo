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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ManagerMetadataUtilTest {

  private AccumuloConfiguration conf;
  private ClientContext context;
  private Ample.TabletMutator tabletMutator;
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
    tabletMutator = EasyMock.createMock(Ample.TabletMutator.class);
  }

  @Test
  public void testUpdateLastForAssignmentModeNullLastLocation() {
    // Expect a put of last1 as the previous value
    EasyMock.expect(tabletMutator.putLocation(last1)).andReturn(tabletMutator).once();
    EasyMock.replay(conf, context, tabletMutator);

    // Pass in a null last location value. There should be a call to
    // tabletMutator.putLocation of last 1 but no deletion as lastLocation is null
    ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator, server1, null);
    EasyMock.verify(conf, context, tabletMutator);
  }

  @Test
  public void testUpdateLastForAssignModeInvalidType() {
    assertThrows(IllegalArgumentException.class, () -> {
      // Should throw an IllegalArgumentException as the lastLocation is not LocationType.LAST
      ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator, server1,
          Location.current(server1));
    });
  }

  @Test
  public void testUpdateLastForAssignModeLastLocationSame() {
    EasyMock.replay(conf, context, tabletMutator);

    // Pass in a last location value that matches the new value of server 1
    // There should be no call to tabletMutator.putLocation or tabletMutator.deleteLocation
    // as the locations are equal so no expects() are defined and any method calls would
    // throw an error
    ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator, server1, last1);
    EasyMock.verify(conf, context, tabletMutator);
  }

  @Test
  public void testUpdateLastForAssignModeLastLocationDifferent() {
    // Expect a delete of last1 as we are providing that as the previous last location
    // which is different from server 2 location
    EasyMock.expect(tabletMutator.deleteLocation(last1)).andReturn(tabletMutator).once();
    EasyMock.expect(tabletMutator.putLocation(last2)).andReturn(tabletMutator).once();

    EasyMock.replay(conf, context, tabletMutator);

    // Pass in last1 as the last location value.
    // There should be no read from Ample as we provided a value as an argument
    // There should be a call to tabletMutator.putLocation and tabletMutator.deleteLocation
    // as the last location is being updated as last1 does not match server 2
    ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator, server2, last1);
    EasyMock.verify(conf, context, tabletMutator);
  }

}
