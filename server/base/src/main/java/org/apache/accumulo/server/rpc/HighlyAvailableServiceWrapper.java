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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.server.HighlyAvailableService;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TBaseProcessor;

/**
 * A class to wrap invocations to the Thrift handler to prevent these invocations from succeeding
 * when the Accumulo service that this Thrift service is for has not yet obtained its ZooKeeper
 * lock.
 *
 * <p>
 * Its expected that all methods in the wrapped thrift service declare they throw
 * {@link org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException}. The methods
 * should declare they throw in the thrift IDL.
 *
 * @since 2.0
 */
public class HighlyAvailableServiceWrapper {

  /**
   * Returns all thrift methods on a processor along w/ an indication if they are oneway or not.
   */
  static <I> Map<String,Boolean> getThriftMethods(TBaseProcessor<I> tbProcessor) {
    Map<String,ProcessFunction<I,?>> pmv = tbProcessor.getProcessMapView();

    Method method;
    try {
      method = ProcessFunction.class.getDeclaredMethod("isOneway");
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          "Failed to find isOneway method on class " + ProcessFunction.class.getName(), e);
    }
    method.setAccessible(true);

    Map<String,Boolean> thriftMethods = new HashMap<>(pmv.size());

    pmv.forEach((m, pf) -> {
      try {
        thriftMethods.put(m, (Boolean) method.invoke(pf));
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    });

    return thriftMethods;
  }

  /**
   * Ensures all non oneway thrift methods throw {@link ThriftNotActiveServiceException}
   */
  private static void validateHAServerExceptions(Class<?> thriftInterface,
      Map<String,Boolean> thriftMethods) {
    String className = thriftInterface.getName();
    Method[] methods = thriftInterface.getDeclaredMethods();
    outer: for (Method m : methods) {
      Class<?>[] exceptionClasses = m.getExceptionTypes();
      Boolean oneway = thriftMethods.get(m.getName());
      if (oneway) {
        continue;
      }

      for (Class<?> ec : exceptionClasses) {
        if (ThriftNotActiveServiceException.class.getName().equals(ec.getName())) {
          continue outer;
        }
      }
      throw new IllegalStateException(
          "Method " + m.getName() + " on " + className + " does not declare "
              + ThriftNotActiveServiceException.class.getSimpleName() + " to be thrown");
    }
  }

  // Not for public use.
  private HighlyAvailableServiceWrapper() {}

  public static <I> I service(Class<I> iface, Function<I,TBaseProcessor<I>> processorFactory,
      final I handler, HighlyAvailableService service) {
    var processor = processorFactory.apply(handler);
    var thriftMethods = getThriftMethods(processor);
    validateHAServerExceptions(iface, thriftMethods);

    var onewayMethods = thriftMethods.entrySet().stream().filter(Map.Entry::getValue)
        .map(Map.Entry::getKey).collect(Collectors.toSet());

    InvocationHandler proxyHandler =
        new HighlyAvailableServiceInvocationHandler<>(handler, service, onewayMethods);
    @SuppressWarnings("unchecked")
    I proxiedInstance = (I) Proxy.newProxyInstance(handler.getClass().getClassLoader(),
        new Class<?>[] {iface}, proxyHandler);
    return proxiedInstance;
  }
}
