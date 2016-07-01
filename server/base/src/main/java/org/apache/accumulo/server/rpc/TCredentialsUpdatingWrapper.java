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
import java.lang.reflect.Proxy;

import org.apache.accumulo.core.conf.AccumuloConfiguration;

/**
 * Utility method to ensure that the instance of TCredentials which is passed to the implementation of a Thrift service has the correct principal from SASL at
 * the Thrift transport layer when SASL/GSSAPI (kerberos) is enabled. This ensures that we use the strong authentication provided to us and disallow any other
 * principal names that client (malicious or otherwise) might pass in.
 */
public class TCredentialsUpdatingWrapper {

  public static <T> T service(final T instance, final Class<? extends T> originalClass, AccumuloConfiguration conf) {
    InvocationHandler handler = new TCredentialsUpdatingInvocationHandler<>(instance, conf);

    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(originalClass.getClassLoader(), originalClass.getInterfaces(), handler);

    return proxiedInstance;
  }

}
