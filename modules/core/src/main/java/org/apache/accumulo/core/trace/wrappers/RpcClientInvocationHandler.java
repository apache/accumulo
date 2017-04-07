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
package org.apache.accumulo.core.trace.wrappers;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;

public class RpcClientInvocationHandler<I> implements InvocationHandler {

  private final I instance;

  protected RpcClientInvocationHandler(final I clientInstance) {
    instance = clientInstance;
  }

  @Override
  public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
    if (args == null || args.length < 1 || args[0] != null) {
      return method.invoke(instance, args);
    }
    Class<?> klass = method.getParameterTypes()[0];
    if (TInfo.class.isAssignableFrom(klass)) {
      args[0] = Tracer.traceInfo();
    }
    Span span = Trace.start("client:" + method.getName());
    try {
      return method.invoke(instance, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    } finally {
      span.stop();
    }
  }
}
