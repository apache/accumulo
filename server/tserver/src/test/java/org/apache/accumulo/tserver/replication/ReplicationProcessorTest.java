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
package org.apache.accumulo.tserver.replication;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.fs.VolumeManager;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class ReplicationProcessorTest {

  @Test
  public void peerTypeExtractionFromConfiguration() {
    Instance inst = EasyMock.createMock(Instance.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    Credentials creds = new Credentials("foo", new PasswordToken("bar"));
    
    Map<String,String> data = new HashMap<>();

    String peerName = "peer";
    String configuration = "java.lang.String,foo";
    data.put(Property.REPLICATION_PEERS + peerName, configuration);
    ConfigurationCopy conf = new ConfigurationCopy(data);

    ReplicationProcessor proc = new ReplicationProcessor(inst, conf, fs, creds);

    Assert.assertEquals(configuration, proc.getPeerType(peerName));
  }

  @Test(expected = IllegalArgumentException.class)
  public void noPeerConfigurationThrowsAnException() {
    Instance inst = EasyMock.createMock(Instance.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    Credentials creds = new Credentials("foo", new PasswordToken("bar"));
    
    Map<String,String> data = new HashMap<>();
    ConfigurationCopy conf = new ConfigurationCopy(data);

    ReplicationProcessor proc = new ReplicationProcessor(inst, conf, fs, creds);

    proc.getPeerType("foo");
  }
}
