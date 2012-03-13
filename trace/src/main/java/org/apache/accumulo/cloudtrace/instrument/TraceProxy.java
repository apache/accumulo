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
package org.apache.accumulo.cloudtrace.instrument;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TraceProxy {
  // private static final Logger log = Logger.getLogger(TraceProxy.class);
  
  static final Sampler ALWAYS = new Sampler() {
    @Override
    public boolean next() {
      return true;
    }
  };
  
  public static <T> T trace(T instance) {
    return trace(instance, ALWAYS);
  }
  
  @SuppressWarnings("unchecked")
  public static <T> T trace(final T instance, final Sampler sampler) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object obj, Method method, Object[] args) throws Throwable {
        if (!sampler.next()) {
          return method.invoke(instance, args);
        }
        Span span = Trace.on(method.getName());
        try {
          return method.invoke(instance, args);
        } catch (Throwable ex) {
          ex.printStackTrace();
          throw ex;
        } finally {
          span.stop();
        }
      }
    };
    return (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(), instance.getClass().getInterfaces(), handler);
  }
  
}
