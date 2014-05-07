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
package org.apache.accumulo.fate.zookeeper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * An invocation handler for ZooKeeper reader/writers that retries calls that fail due to connection loss.
 */
public class RetryingInvocationHandler implements InvocationHandler {
  private final IZooReaderWriter zrw;

  /**
   * Creates a new invocation handler.
   *
   * @param zrw
   *          ZooKeeper reader/writer being handled
   */
  public RetryingInvocationHandler(IZooReaderWriter zrw) {
    this.zrw = zrw;
  }

  private static final long INITIAL_RETRY_TIME = 250L;
  private static final long RETRY_INCREMENT = 250L;
  private static final long MAXIMUM_RETRY_TIME = 5000L;

  @Override
  public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
    long retryTime = INITIAL_RETRY_TIME;
    while (true) {
      try {
        return method.invoke(zrw, args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof KeeperException.ConnectionLossException) {
          Logger.getLogger(ZooReaderWriter.class).warn("Error connecting to zookeeper, will retry in " + String.format("%.2f secs", retryTime / 1000.0), e.getCause());
          UtilWaitThread.sleep(retryTime);
          retryTime = Math.min(MAXIMUM_RETRY_TIME, retryTime + RETRY_INCREMENT);
        } else {
          throw e.getCause();
        }
      }
    }
  }
}
