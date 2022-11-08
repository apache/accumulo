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
package org.apache.accumulo.manager.tableOps;

import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;

public abstract class ManagerRepo implements Repo<Manager> {

  private static final long serialVersionUID = 1L;

  @Override
  public long isReady(long tid, Manager environment) throws Exception {
    return 0;
  }

  @Override
  public void undo(long tid, Manager environment) throws Exception {}

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public String getReturn() {
    return null;
  }

  @Override
  public abstract Repo<Manager> call(long tid, Manager environment) throws Exception;

}
