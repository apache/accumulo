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
package org.apache.accumulo.server.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.trace.wrappers.RpcServerInvocationHandler;
import org.apache.accumulo.core.trace.wrappers.TraceWrap;
import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class accommodates the changes in THRIFT-1805, which appeared in Thrift 0.9.1 and restricts
 * client-side notification of server-side errors to {@link TException} only, by wrapping
 * {@link RuntimeException} and {@link Error} as {@link TException}, so it doesn't just close the
 * connection and look like a network issue, but informs the client that a
 * {@link TApplicationException} had occurred, as it did in Thrift 0.9.0. This performs similar
 * functions as {@link TraceWrap}, but with the additional action of translating exceptions. See
 * also ACCUMULO-1691 and ACCUMULO-2950.
 *
 * ACCUMULO-4065 found that the above exception-wrapping is not appropriate for Thrift's
 * implementation of oneway methods. Oneway methods are defined as a method which the client does
 * not wait for it to return. Normally, this is acceptable as these methods are void. Therefore, if
 * another client reuses the connection to send a new RPC, there is no "extra" data sitting on the
 * InputStream from the Socket (that the server sent). However, the implementation of a oneway
 * method <em>does</em> send a response to the client when the implementation throws a
 * {@link TException}. This message showing up on the client's InputStream causes future use of the
 * Thrift Connection to become unusable. As long as the Thrift implementation sends a message back
 * when oneway methods throw a {@link TException}, we much make sure that we don't re-wrap-and-throw
 * any exceptions as {@link TException}s.
 *
 * @since 1.6.1
 */
public class RpcWrapper {
  private static final Logger log = LoggerFactory.getLogger(RpcWrapper.class);

  public static <I> I service(final I instance, final TBaseProcessor<I> processor) {
    final Map<String,ProcessFunction<I,?>> processorView = processor.getProcessMapView();
    final Set<String> onewayMethods = getOnewayMethods(processorView);
    log.debug("Found oneway Thrift methods: " + onewayMethods);

    InvocationHandler handler = getInvocationHandler(instance, onewayMethods);

    @SuppressWarnings("unchecked")
    I proxiedInstance = (I) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

  protected static <T> RpcServerInvocationHandler<T> getInvocationHandler(final T instance,
      final Set<String> onewayMethods) {
    return new RpcServerInvocationHandler<T>(instance) {
      private final Logger log = LoggerFactory.getLogger(instance.getClass());

      @Override
      public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
        // e.g. ThriftClientHandler.flush(TInfo, TCredentials, ...)
        try {
          return super.invoke(obj, method, args);
        } catch (RuntimeException e) {
          String msg = e.getMessage();
          log.error(msg, e);
          if (onewayMethods.contains(method.getName())) {
            throw e;
          }
          throw new TException(msg);
        } catch (Error e) {
          String msg = e.getMessage();
          log.error(msg, e);
          if (onewayMethods.contains(method.getName())) {
            throw e;
          }
          throw new TException(msg);
        }
      }
    };
  }

  protected static Set<String> getOnewayMethods(Map<String,?> processorView) {
    // Get a handle on the isOnewayMethod and make it accessible
    final Method isOnewayMethod;
    try {
      isOnewayMethod = ProcessFunction.class.getDeclaredMethod("isOneway");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Could not access isOneway method", e);
    } catch (SecurityException e) {
      throw new RuntimeException("Could not access isOneway method", e);
    }
    // In java7, this appears to be copying the method, but it's trivial for us to return the object
    // to how it was before.
    final boolean accessible = isOnewayMethod.isAccessible();
    isOnewayMethod.setAccessible(true);

    try {
      final Set<String> onewayMethods = new HashSet<>();
      for (Entry<String,?> entry : processorView.entrySet()) {
        try {
          if ((Boolean) isOnewayMethod.invoke(entry.getValue())) {
            onewayMethods.add(entry.getKey());
          }
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      return onewayMethods;
    } finally {
      // Reset it back to how it was.
      isOnewayMethod.setAccessible(accessible);
    }
  }
}
