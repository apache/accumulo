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
package org.apache.accumulo.core.dataImpl;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.core.data.InstanceId;

public class InstanceInfo {

  private final String name;
  private final InstanceId id;

  public InstanceInfo(String name, InstanceId id) {
    this.name = requireNonNull(name);
    this.id = requireNonNull(id);
  }

  public String getInstanceName() {
    return name;
  }

  public InstanceId getInstanceId() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InstanceInfo) {
      var o = (InstanceInfo) obj;
      return Objects.equals(getInstanceName(), o.getInstanceName())
          && Objects.equals(getInstanceId(), o.getInstanceId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getInstanceName(), getInstanceId());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[name=" + getInstanceName() + ";id=" + getInstanceId()
        + "]";
  }

}
