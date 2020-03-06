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
package org.apache.accumulo.server.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;

import com.google.common.annotations.VisibleForTesting;

public class ZooReaderWriter extends org.apache.accumulo.fate.zookeeper.ZooReaderWriter {
  private static final String SCHEME = "digest";
  private static final String USER = "accumulo";
  private static ZooReaderWriter instance = null;

  public ZooReaderWriter(String string, int timeInMillis, String secret) {
    super(string, timeInMillis, SCHEME, (USER + ":" + secret).getBytes(UTF_8));
  }

  public static synchronized ZooReaderWriter getInstance() {
    if (instance == null) {
      AccumuloConfiguration conf = SiteConfiguration.getInstance();
      instance = new ZooReaderWriter(conf.get(Property.INSTANCE_ZK_HOST),
          (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
          conf.get(Property.INSTANCE_SECRET));
    }
    return instance;
  }

  /**
   * This method exposes the zookeeper connection parameters when instantiating a cached singleton
   * ZooReaderWriter instance for later use in testing. The instance will only be created once,
   * otherwise the previously cached instance is returned.
   *
   * @param zooConnString
   *          a zookeeper connection string of host:port pair of server(s)
   * @param timeInMillis
   *          the zooKeeper connection timeout
   * @param secret
   *          the Accumulo instance secret
   * @return a ZooReaderWriter instance either created or previously cached.
   */
  @VisibleForTesting
  public static synchronized ZooReaderWriter getInstance(String zooConnString, int timeInMillis,
      String secret) {
    if (instance == null) {
      instance = new ZooReaderWriter(zooConnString, timeInMillis, secret);
    }
    return instance;
  }
}
