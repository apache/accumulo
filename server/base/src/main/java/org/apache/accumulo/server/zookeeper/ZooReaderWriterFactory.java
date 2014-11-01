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
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;

/**
 * A factory for {@link ZooReaderWriter} objects.
 */
public class ZooReaderWriterFactory {
  private static final String SCHEME = "digest";
  private static final String USER = "accumulo";
  private static IZooReaderWriter instance = null;

  /**
   * Gets a new reader/writer.
   *
   * @param string
   *          ZooKeeper connection string
   * @param timeInMillis
   *          session timeout in milliseconds
   * @param secret
   *          instance secret
   * @return reader/writer
   */
  public IZooReaderWriter getZooReaderWriter(String string, int timeInMillis, String secret) {
    return new ZooReaderWriter(string, timeInMillis, SCHEME, (USER + ":" + secret).getBytes(UTF_8));
  }

  /**
   * Gets a reader/writer, retrieving ZooKeeper information from the site configuration. The same instance may be returned for multiple calls.
   *
   * @return reader/writer
   */
  public IZooReaderWriter getInstance() {
    synchronized (ZooReaderWriterFactory.class) {
      if (instance == null) {
        AccumuloConfiguration conf = SiteConfiguration.getInstance();
        instance = getZooReaderWriter(conf.get(Property.INSTANCE_ZK_HOST), (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
            conf.get(Property.INSTANCE_SECRET));
      }
      return instance;
    }
  }
}
