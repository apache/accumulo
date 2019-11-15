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
package org.apache.accumulo.core.clientImpl;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.accumulo.core.conf.ClientProperty;
import org.junit.Test;

public class ClientConfConverterTest {

  @Test
  public void testBasic() {
    Properties before = new Properties();
    before.setProperty(ClientProperty.INSTANCE_NAME.getKey(), "instance");
    before.setProperty(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), "zookeepers");
    ClientProperty.setPassword(before, "mypass");
    before.setProperty(ClientProperty.SSL_ENABLED.getKey(), "true");
    before.setProperty(ClientProperty.SSL_KEYSTORE_PATH.getKey(), "key_path");
    before.setProperty(ClientProperty.SSL_KEYSTORE_PASSWORD.getKey(), "key_pass");
    before.setProperty(ClientProperty.SSL_TRUSTSTORE_PATH.getKey(), "trust_path");
    before.setProperty(ClientProperty.SASL_ENABLED.getKey(), "true");
    before.setProperty(ClientProperty.SASL_KERBEROS_SERVER_PRIMARY.getKey(), "primary");
    before.setProperty(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey(), "5");

    Properties after = ClientConfConverter.toProperties(ClientConfConverter.toClientConf(before));
    assertEquals(before, after);
  }
}
