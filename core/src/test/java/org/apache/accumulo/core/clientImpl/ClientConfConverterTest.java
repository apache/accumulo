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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Properties;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.junit.jupiter.api.Test;

public class ClientConfConverterTest {

  @Test
  public void testBasic() {
    Properties before = new Properties();

    // this will be dropped when converting to AccumuloConfiguration
    before.setProperty(ClientProperty.INSTANCE_NAME.getKey(), "instance");
    ClientProperty.setPassword(before, "mypass");
    before.setProperty(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey(), "5");

    // these will map to equivalent in AccumuloConfiguration
    before.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), "zookeepers");
    before.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), "20s");
    before.setProperty(ClientProperty.SSL_ENABLED.getKey(), "true");
    before.setProperty(ClientProperty.SSL_USE_JSSE.getKey(), "true");
    before.setProperty(ClientProperty.SSL_KEYSTORE_PATH.getKey(), "key_path");
    before.setProperty(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(), "key_pass");
    before.setProperty(ClientProperty.SSL_KEYSTORE_TYPE.getKey(), "jks");
    before.setProperty(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(), "trust_path");
    before.setProperty(ClientProperty.SSL_TRUSTSTORE_PASSWORD.getKey(), "trust_pass");
    before.setProperty(ClientProperty.SSL_TRUSTSTORE_TYPE.getKey(), "jks");
    before.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
    before.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), "primary");
    before.setProperty(ClientProperty.SASL_QOP.getKey(), "auth-int");

    Properties after = ClientConfConverter.toProperties(ClientConfConverter.toAccumuloConf(before));

    // some props don't have an equivalent in the AccumuloConfiguration; set them here and check
    assertNotEquals(before, after);
    ClientProperty.setPassword(after, "mypass");

    assertNotEquals(before, after);
    after.setProperty(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey(), "5");

    assertNotEquals(before, after);
    after.setProperty(ClientProperty.INSTANCE_NAME.getKey(), "instance");

    assertEquals(before, after);
  }

  // this test ensures a general property can be set and used by a client
  @Test
  public void testGeneralPropsWorkAsClientProperties() {
    Property prop = Property.GENERAL_RPC_TIMEOUT;
    Properties fromUser = new Properties();
    fromUser.setProperty(prop.getKey(), "5s");
    AccumuloConfiguration converted = ClientConfConverter.toAccumuloConf(fromUser);

    // verify that converting client props actually picked up and overrode the default
    assertNotEquals(converted.getTimeInMillis(prop),
        DefaultConfiguration.getInstance().getTimeInMillis(prop));
    // verify that it was set to the expected value set in the client props
    assertEquals(SECONDS.toMillis(5), converted.getTimeInMillis(prop));
  }
}
