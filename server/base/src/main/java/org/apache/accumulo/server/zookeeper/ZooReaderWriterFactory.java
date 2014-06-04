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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.Charset;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.RetryingInvocationHandler;
import org.apache.accumulo.server.conf.ServerConfiguration;

/**
 * A factory for {@link ZooReaderWriter} objects.
 */
public class ZooReaderWriterFactory {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String SCHEME = "digest";
  private static final String USER = "accumulo";
  private static IZooReaderWriter instance = null;
  private static IZooReaderWriter retryingInstance = null;

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
    return new ZooReaderWriter(string, timeInMillis, SCHEME, (USER + ":" + secret).getBytes(UTF8));
  }

  /**
   * Gets a reader/writer, retrieving ZooKeeper information from the site configuration. The same instance may be returned for multiple calls.
   *
   * @return reader/writer
   */
  public IZooReaderWriter getInstance() {
    synchronized (ZooReaderWriterFactory.class) {
      if (instance == null) {
        AccumuloConfiguration conf = ServerConfiguration.getSiteConfiguration();
        instance = getZooReaderWriter(conf.get(Property.INSTANCE_ZK_HOST), (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
            conf.get(Property.INSTANCE_SECRET));
      }
      return instance;
    }
  }

  /**
   * Gets a reader/writer, retrieving ZooKeeper information from the site configuration, and that retries on connection loss. The same instance may be returned
   * for multiple calls.
   *
   * @return retrying reader/writer
   */
  public IZooReaderWriter getRetryingInstance() {
    synchronized (ZooReaderWriterFactory.class) {
      if (retryingInstance == null) {
        IZooReaderWriter inst = getInstance();
        InvocationHandler ih = new RetryingInvocationHandler(inst);
        retryingInstance = (IZooReaderWriter) Proxy.newProxyInstance(IZooReaderWriter.class.getClassLoader(), new Class[] {IZooReaderWriter.class}, ih);
      }
      return retryingInstance;
    }
  }
}
