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
package org.apache.accumulo.tserver.data;

import java.util.List;

import org.apache.accumulo.core.dataImpl.thrift.TCondition;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalMutation;
import org.apache.accumulo.server.data.ServerMutation;

public class ServerConditionalMutation extends ServerMutation {

  private long cmid;
  private List<TCondition> conditions;

  public ServerConditionalMutation(TConditionalMutation input) {
    super(input.mutation);

    this.cmid = input.id;
    this.conditions = input.conditions;
  }

  public long getID() {
    return cmid;
  }

  public List<TCondition> getConditions() {
    return conditions;
  }

}
