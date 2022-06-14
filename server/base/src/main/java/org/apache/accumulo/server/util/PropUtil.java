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
package org.apache.accumulo.server.util;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.server.ServerContext;

public abstract class PropUtil<T extends AbstractId<T>> {

  protected final ServerContext context;

  protected PropUtil(ServerContext context) {
    this.context = context;
  }

  public abstract void setProperties(final T id, final Map<String,String> props);

  public abstract void removeProperties(final T id, final Collection<String> propertyNames);

}
