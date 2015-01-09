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
package org.apache.accumulo.server.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.accumulo.trace.instrument.thrift.RpcServerInvocationHandler;
import org.apache.accumulo.trace.instrument.thrift.TraceWrap;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class accommodates the changes in THRIFT-1805, which appeared in Thrift 0.9.1 and restricts client-side notification of server-side errors to
 * {@link TException} only, by wrapping {@link RuntimeException} and {@link Error} as {@link TException}, so it doesn't just close the connection and look like
 * a network issue, but informs the client that a {@link TApplicationException} had occurred, as it did in Thrift 0.9.0. This performs similar functions as
 * {@link TraceWrap}, but with the additional action of translating exceptions. See also ACCUMULO-1691 and ACCUMULO-2950.
 *
 * @since 1.6.1
 */
public class RpcWrapper {

  public static <T> T service(final T instance) {
    InvocationHandler handler = new RpcServerInvocationHandler<T>(instance) {
      private final Logger log = LoggerFactory.getLogger(instance.getClass());

      @Override
      public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
        try {
          return super.invoke(obj, method, args);
        } catch (RuntimeException e) {
          String msg = e.getMessage();
          log.error(msg, e);
          throw new TException(msg);
        } catch (Error e) {
          String msg = e.getMessage();
          log.error(msg, e);
          throw new TException(msg);
        }
      }
    };

    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(), instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

}
