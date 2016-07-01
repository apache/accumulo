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
package org.apache.accumulo.tserver.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.tserver.TservConstraintEnv;
import org.apache.accumulo.tserver.tablet.Tablet;

public class UpdateSession extends Session {
  public final TservConstraintEnv cenv;
  public final MapCounter<Tablet> successfulCommits = new MapCounter<>();
  public final Map<KeyExtent,Long> failures = new HashMap<>();
  public final HashMap<KeyExtent,SecurityErrorCode> authFailures = new HashMap<>();
  public final Stat prepareTimes = new Stat();
  public final Stat walogTimes = new Stat();
  public final Stat commitTimes = new Stat();
  public final Stat authTimes = new Stat();
  public final Map<Tablet,List<Mutation>> queuedMutations = new HashMap<>();
  public final Violations violations;

  public Tablet currentTablet = null;
  public long totalUpdates = 0;
  public long flushTime = 0;
  public long queuedMutationSize = 0;
  public final Durability durability;

  public UpdateSession(TservConstraintEnv env, TCredentials credentials, Durability durability) {
    super(credentials);
    this.cenv = env;
    this.violations = new Violations();
    this.durability = durability;
  }
}
