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
package org.apache.accumulo.core.client.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class ReplicaSystemFactory {
  private static final Logger log = LoggerFactory.getLogger(ReplicaSystemFactory.class);

  /**
   * @param value {@link ReplicaSystem} implementation class name
   * @return A {@link ReplicaSystem} object from the given name
   */
  public static ReplicaSystem get(String value) {
    Preconditions.checkNotNull(value);

    int index = value.indexOf(',');
    if (-1 == index) {
      throw new IllegalArgumentException("Expected comma separator between replication system name and configuration");
    }

    String name = value.substring(0, index);
    String configuration = value.substring(index + 1);
    
    try {
      Class<?> clz = Class.forName(name);
      Object o = clz.newInstance();

      if (ReplicaSystem.class.isAssignableFrom(o.getClass())) {
        ReplicaSystem rs = (ReplicaSystem) o;
        rs.configure(configuration);
        return rs;
      }

      throw new RuntimeException("Class is not assignable to ReplicaSystem: " + name);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      log.error("Error creating ReplicaSystem object", e);
      throw new RuntimeException(e);
    }
  }
}
