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

import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.fate.Repo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ManagerRepo implements Repo {
  protected Logger logger = LoggerFactory.getLogger(ManagerRepo.class);

  private static final long serialVersionUID = 1L;

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  public abstract Repo call(long tid, Manager environment) throws Exception;

  protected Logger getLogger() {
    return logger;
  }
}
