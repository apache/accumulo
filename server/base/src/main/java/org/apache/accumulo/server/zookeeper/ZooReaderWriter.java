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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class ZooReaderWriter extends org.apache.accumulo.fate.zookeeper.ZooReaderWriter {
  private static final String SCHEME = "digest";
  private static final String USER = "accumulo";
  private static ZooReaderWriter instance = null;
  private static IZooReaderWriter retryingInstance = null;
  
  public ZooReaderWriter(String string, int timeInMillis, String secret) {
    super(string, timeInMillis, SCHEME, (USER + ":" + secret).getBytes());
  }
  
  public static synchronized ZooReaderWriter getInstance() {
    if (instance == null) {
      AccumuloConfiguration conf = ServerConfiguration.getSiteConfiguration();
      instance = new ZooReaderWriter(conf.get(Property.INSTANCE_ZK_HOST), (int) conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT),
          conf.get(Property.INSTANCE_SECRET));
    }
    return instance;
  }
  
  /**
   * get an instance that retries when zookeeper connection errors occur
   * 
   * @return an instance that retries when Zookeeper connection errors occur.
   */
  public static synchronized IZooReaderWriter getRetryingInstance() {
    
    if (retryingInstance == null) {
      final IZooReaderWriter inst = getInstance();
      
      InvocationHandler ih = new InvocationHandler() {
        @Override
        public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
          long retryTime = 250;
          while (true) {
            try {
              return method.invoke(inst, args);
            } catch (InvocationTargetException e) {
              if (e.getCause() instanceof KeeperException.ConnectionLossException) {
                Logger.getLogger(ZooReaderWriter.class).warn("Error connecting to zookeeper, will retry in " + retryTime, e.getCause());
                UtilWaitThread.sleep(retryTime);
                retryTime = Math.min(5000, retryTime + 250);
              } else {
                throw e.getCause();
              }
            }
          }
        }
      };
      
      retryingInstance = (IZooReaderWriter) Proxy.newProxyInstance(ZooReaderWriter.class.getClassLoader(), new Class[] {IZooReaderWriter.class}, ih);
    }
    
    return retryingInstance;
  }
}
