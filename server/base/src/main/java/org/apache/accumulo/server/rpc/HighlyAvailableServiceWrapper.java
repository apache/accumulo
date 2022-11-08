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
package org.apache.accumulo.server.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import org.apache.accumulo.server.HighlyAvailableService;

/**
 * A class to wrap invocations to the Thrift handler to prevent these invocations from succeeding
 * when the Accumulo service that this Thrift service is for has not yet obtained its ZooKeeper
 * lock.
 *
 * @since 2.0
 */
public class HighlyAvailableServiceWrapper {

  private static final HighlyAvailableServiceWrapper INSTANCE = new HighlyAvailableServiceWrapper();

  // Not for public use.
  private HighlyAvailableServiceWrapper() {}

  public static <I> I service(final I instance, HighlyAvailableService service) {
    InvocationHandler handler = INSTANCE.getInvocationHandler(instance, service);

    @SuppressWarnings("unchecked")
    I proxiedInstance = (I) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

  protected <T> HighlyAvailableServiceInvocationHandler<T> getInvocationHandler(final T instance,
      final HighlyAvailableService service) {
    return new HighlyAvailableServiceInvocationHandler<>(instance, service);
  }
}
