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
package org.apache.accumulo.server.fs;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterfaceEvolutionWarner {

  private static final Logger LOG = LoggerFactory.getLogger("accumulo.api.evolution");
  private static final ConcurrentHashMap<String,Boolean> classesWarned = new ConcurrentHashMap<>();

  static void warnOnce(Class<?> userClass, Class<?> interfaceClass, String interfaceMethod,
      String version) {
    String userClassName = userClass.getName();
    String interfaceSignature = interfaceClass.getName() + "#" + interfaceMethod;
    // don't warn multiple times for the same user class and interface method signature
    String key = userClassName + ":" + interfaceSignature;
    if (classesWarned.putIfAbsent(key, Boolean.TRUE) == null) {
      LOG.warn("{} must override {} by {}", userClassName, interfaceSignature, version);
    }
  }

}
