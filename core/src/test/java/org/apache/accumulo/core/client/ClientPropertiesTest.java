/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.conf.ClientProperty;
import org.junit.Test;

public class ClientPropertiesTest {

  @Test
  public void testBasic() {
    Properties props1 =
        Accumulo.newClientProperties().to("inst1", "zoo1").as("user1", "pass1").build();
    assertEquals("inst1", ClientProperty.INSTANCE_NAME.getValue(props1));
    assertEquals("zoo1", ClientProperty.INSTANCE_ZOOKEEPERS.getValue(props1));
    assertEquals("user1", ClientProperty.AUTH_PRINCIPAL.getValue(props1));
    assertEquals("password", ClientProperty.AUTH_TYPE.getValue(props1));
    assertEquals("pass1", ClientProperty.AUTH_TOKEN.getValue(props1));

    Properties props2 =
        Accumulo.newClientProperties().from(props1).as("user2", Paths.get("/path2")).build();
    assertEquals("inst1", ClientProperty.INSTANCE_NAME.getValue(props1));
    assertEquals("zoo1", ClientProperty.INSTANCE_ZOOKEEPERS.getValue(props1));
    assertEquals("user2", ClientProperty.AUTH_PRINCIPAL.getValue(props1));
    assertEquals("kerberos", ClientProperty.AUTH_TYPE.getValue(props1));
    assertEquals("/path2", ClientProperty.AUTH_TOKEN.getValue(props1));

    props2.remove(ClientProperty.AUTH_PRINCIPAL.getKey());
    try {
      ClientProperty.validate(props2);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
