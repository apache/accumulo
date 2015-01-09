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
package org.apache.accumulo.trace.instrument.thrift;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * To move trace data from client to server, the RPC call must be annotated to take a TInfo object as its first argument. The user can simply pass null, so long
 * as they wrap their Client and Service objects with these functions.
 *
 * <pre>
 * Trace.on(&quot;remoteMethod&quot;);
 * Iface c = new Client();
 * c = TraceWrap.client(c);
 * c.remoteMethod(null, arg2, arg3);
 * Trace.off();
 * </pre>
 *
 * The wrapper will see the annotated method and send or re-establish the trace information.
 *
 * Note that the result of these calls is a Proxy object that conforms to the basic interfaces, but is not your concrete instance.
 *
 */
public class TraceWrap {

  public static <T> T service(final T instance) {
    InvocationHandler handler = new RpcServerInvocationHandler<T>(instance);
    return wrappedInstance(handler, instance);
  }

  public static <T> T client(final T instance) {
    InvocationHandler handler = new RpcClientInvocationHandler<T>(instance);
    return wrappedInstance(handler, instance);
  }

  private static <T> T wrappedInstance(final InvocationHandler handler, final T instance) {
    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(), instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

}
