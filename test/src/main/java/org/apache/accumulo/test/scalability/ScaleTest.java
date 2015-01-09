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
package org.apache.accumulo.test.scalability;

import java.util.Properties;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;

public abstract class ScaleTest {

  private Connector conn;
  private Properties scaleProps;
  private Properties testProps;
  private int numTabletServers;
  private long startTime;

  public void init(Properties scaleProps, Properties testProps, int numTabletServers) throws AccumuloException, AccumuloSecurityException {

    this.scaleProps = scaleProps;
    this.testProps = testProps;
    this.numTabletServers = numTabletServers;

    // get properties to create connector
    String instanceName = this.scaleProps.getProperty("INSTANCE_NAME");
    String zookeepers = this.scaleProps.getProperty("ZOOKEEPERS");
    String user = this.scaleProps.getProperty("USER");
    String password = this.scaleProps.getProperty("PASSWORD");
    System.out.println(password);

    conn = new ZooKeeperInstance(new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeepers)).getConnector(user, new PasswordToken(password));
  }

  protected void startTimer() {
    startTime = System.currentTimeMillis();
  }

  protected void stopTimer(long numEntries, long numBytes) {
    long endTime = System.currentTimeMillis();
    System.out.printf("ELAPSEDMS %d %d %d%n", endTime - startTime, numEntries, numBytes);
  }

  public abstract void setup();

  public abstract void client();

  public abstract void teardown();

  public TreeSet<Text> calculateSplits() {
    int numSplits = numTabletServers - 1;
    long distance = (Long.MAX_VALUE / numTabletServers) + 1;
    long split = distance;
    TreeSet<Text> keys = new TreeSet<Text>();
    for (int i = 0; i < numSplits; i++) {
      keys.add(new Text(String.format("%016x", split)));
      split += distance;
    }
    return keys;
  }

  public Connector getConnector() {
    return conn;
  }

  public String getTestProperty(String key) {
    return testProps.getProperty(key);
  }
}
