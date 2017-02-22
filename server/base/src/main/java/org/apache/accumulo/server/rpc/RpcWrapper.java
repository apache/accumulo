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

import org.apache.accumulo.core.trace.wrappers.RpcServerInvocationHandler;
import org.apache.accumulo.core.trace.wrappers.TraceWrap;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

/**
 * This class accommodates the changes in THRIFT-1805, which appeared in Thrift 0.9.1 and restricts client-side notification of server-side errors to
 * {@link TException} only, by wrapping {@link RuntimeException} and {@link Error} as {@link TException}, so it doesn't just close the connection and look like
 * a network issue, but informs the client that a {@link TApplicationException} had occurred, as it did in Thrift 0.9.0. This performs similar functions as
 * {@link TraceWrap}, but with the additional action of translating exceptions. See also ACCUMULO-1691 and ACCUMULO-2950.
 *
 * ACCUMULO-4065 found that the above exception-wrapping is not appropriate for Thrift's implementation of oneway methods. Oneway methods are defined as a
 * method which the client does not wait for it to return. Normally, this is acceptable as these methods are void. Therefore, if another client reuses the
 * connection to send a new RPC, there is no "extra" data sitting on the InputStream from the Socket (that the server sent). However, the implementation of a
 * oneway method <em>does</em> send a response to the client when the implementation throws a {@link TException}. This message showing up on the client's
 * InputStream causes future use of the Thrift Connection to become unusable. As long as the Thrift implementation sends a message back when oneway methods
 * throw a {@link TException}, we much make sure that we don't re-wrap-and-throw any exceptions as {@link TException}s.
 *
 * @since 1.6.1
 */
public class RpcWrapper {

  public static <I> I service(final I instance) {
    InvocationHandler handler = getInvocationHandler(instance);

    @SuppressWarnings("unchecked")
    I proxiedInstance = (I) Proxy.newProxyInstance(instance.getClass().getClassLoader(), instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

  protected static <T> RpcServerInvocationHandler<T> getInvocationHandler(final T instance) {
    return new RpcServerInvocationHandler<T>(instance) {

      @Override
      public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
        // e.g. ThriftClientHandler.flush(TInfo, TCredentials, ...)
        try {
          return super.invoke(obj, method, args);
        } catch (RuntimeException e) {
          // thrift will log the exception in ProcessFunction
          throw new TException(e);
        }
      }
    };
  }
}
