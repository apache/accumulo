/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.monitor;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

public class EmbeddedWebServerTest {

  @Test
  public void emptyStoreTypeIsNotSet() {
    // This test is intentionally brittle. If the default keystore/truststore type configuration
    // value changes, the implementation and this test should also change.
    ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    // If we do not set passwords, Jetty will prompt for a password on stdin
    conf.set(Property.MONITOR_SSL_KEYSTOREPASS, "password");
    conf.set(Property.MONITOR_SSL_TRUSTSTOREPASS, "password");

    assertEquals("", conf.get(Property.MONITOR_SSL_KEYSTORETYPE));
    assertEquals("", conf.get(Property.MONITOR_SSL_TRUSTSTORETYPE));

    SslContextFactory sslContextFactory = new SslContextFactory();

    // This is also intentionally brittle for the same reason above.
    assertEquals("JKS", sslContextFactory.getKeyStoreType());
    assertEquals("JKS", sslContextFactory.getTrustStoreType());

    EmbeddedWebServer.configureSslContextFactory(sslContextFactory, conf);

    // Should not be empty ("").
    assertEquals("JKS", sslContextFactory.getKeyStoreType());
    assertEquals("JKS", sslContextFactory.getTrustStoreType());
  }
}
