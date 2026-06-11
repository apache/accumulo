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
package org.apache.accumulo.test.rpc;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * This IT is intended to verify that our Thrift RPC handler implementation methods only declare the
 * exact exceptions defined in the Thrift IDL files, minus the generic TException, which should
 * never be declared directly. Unexpected exception handling behavior can occur if throwing
 * TExceptions that are not declared in a Thrift IDL, and the generic TException is thrown on every
 * RPC interface method, but it has special handling and is intended only for things like libthrift
 * handling of network issues or runtime exceptions thrown from an application.
 */
@Tag(SUNNY_DAY)
@Timeout(value = 15, unit = SECONDS)
public class ThriftExceptionsCheckIT {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftExceptionsCheckIT.class);

  private static class RpcServiceAndInterface {
    private final ClassInfo info;
    private final Class<?> loadedClass;
    private final Class<?> matchingInterface;

    private RpcServiceAndInterface(ClassInfo info, Class<?> loadedClass,
        Class<?> matchingInterface) {
      this.info = info;
      this.loadedClass = loadedClass;
      this.matchingInterface = matchingInterface;
    }
  }

  @Test
  public void checkThriftServices() throws IOException, ClassNotFoundException {
    var allClasses = ClassPath.from(ClassLoader.getSystemClassLoader()).getAllClasses();
    var rpcInterfaces = new HashSet<String>();
    var rpcServiceImplCandidates = new ArrayList<ClassInfo>();

    allClasses.forEach(info -> {
      String name = info.getName();
      if (name.startsWith("org.apache.accumulo.") && name.contains(".thrift.")
          && (name.endsWith("$Iface") || name.endsWith("$AsyncIface"))) {
        rpcInterfaces.add(name);
      } else {
        rpcServiceImplCandidates.add(info);
      }
    });
    assertFalse(allClasses.isEmpty());
    assertFalse(rpcInterfaces.isEmpty());
    assertFalse(rpcServiceImplCandidates.isEmpty());
    assertEquals(allClasses.size(), rpcInterfaces.size() + rpcServiceImplCandidates.size());

    // remove stuff we don't care about
    rpcServiceImplCandidates.removeIf(c -> c.getSimpleName().equals("module-info"));
    rpcServiceImplCandidates.removeIf(c -> !c.getName().startsWith("org.apache.accumulo."));
    rpcServiceImplCandidates.removeIf(c -> c.getName().contains(".thrift.")
        && (c.getName().endsWith("$Client") || c.getName().endsWith("$AsyncClient")));
    rpcServiceImplCandidates
        .removeIf(c -> c.getName().equals(SimpleThriftServiceHandler.class.getName()));

    // identify all the RPC handler classes (those implementing a Thrift Iface)
    var rpcServiceImpls = new ArrayList<RpcServiceAndInterface>();
    for (var c : rpcServiceImplCandidates) {
      Class<?> loaded = c.load();
      for (var i : loaded.getInterfaces()) {
        String iName = i.getName();
        if (rpcInterfaces.contains(iName)) {
          rpcServiceImpls.add(new RpcServiceAndInterface(c, loaded, i));
          LOG.trace("{} implements {}", c.getName(), iName);
        }
      }
    }

    assertFalse(rpcServiceImpls.isEmpty());

    // check each method
    rpcServiceImpls.forEach(ThriftExceptionsCheckIT::checkClass);
  }

  private static void checkClass(RpcServiceAndInterface tuple) {
    for (var method : tuple.loadedClass.getDeclaredMethods()) {
      try {
        var interfaceMethod =
            tuple.matchingInterface.getDeclaredMethod(method.getName(), method.getParameterTypes());
        Set<String> interfaceExceptions =
            Stream.of(interfaceMethod.getExceptionTypes()).map(Class::getName)
                .filter(n -> !n.equals("org.apache.thrift.TException")).collect(Collectors.toSet());
        Set<String> implExceptions =
            Stream.of(method.getExceptionTypes()).map(Class::getName).collect(Collectors.toSet());
        implExceptions.removeAll(interfaceExceptions);
        assertTrue(implExceptions.isEmpty(),
            () -> String.format("%s#%s throws exceptions not defined in the Thrift IDL (%s)",
                tuple.info.getSimpleName(), method.getName(), implExceptions.toString()));
      } catch (NoSuchMethodException e) {
        // method does not exist in the interface, so skip it
      }
    }
  }

}
