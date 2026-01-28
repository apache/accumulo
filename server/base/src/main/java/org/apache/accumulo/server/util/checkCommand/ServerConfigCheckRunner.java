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
package org.apache.accumulo.server.util.checkCommand;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;

public class ServerConfigCheckRunner implements CheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.SERVER_CONFIG;

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    log.trace("********** Checking server configuration **********");

    log.trace("Checking that all configured properties are valid (valid key and value)");
    final Map<String,String> definedProps = new HashMap<>();
    final var config = context.getConfiguration();
    config.getProperties(definedProps, s -> true);
    for (var entry : definedProps.entrySet()) {
      var key = entry.getKey();
      var val = entry.getValue();
      if (!Property.isValidProperty(key, val)) {
        log.warn("Invalid property (key={} val={}) found in the config", key, val);
        status = Admin.CheckCommand.CheckStatus.FAILED;
      }
    }

    log.trace("Checking that all required config properties are present");
    // there are many properties that should be set (default value or user set), identifying them
    // all and checking them here is unrealistic. Some property that is not set but is expected
    // will likely result in some sort of failure eventually anyway. We will just check a few
    // obvious required properties here.
    Set<Property> requiredProps = Set.of(Property.INSTANCE_ZK_HOST, Property.INSTANCE_ZK_TIMEOUT,
        Property.INSTANCE_SECRET, Property.INSTANCE_VOLUMES, Property.GENERAL_THREADPOOL_SIZE,
        Property.GENERAL_DELEGATION_TOKEN_LIFETIME,
        Property.GENERAL_DELEGATION_TOKEN_UPDATE_INTERVAL, Property.GENERAL_IDLE_PROCESS_INTERVAL,
        Property.GENERAL_LOW_MEM_DETECTOR_INTERVAL, Property.GENERAL_LOW_MEM_DETECTOR_THRESHOLD,
        Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL, Property.MANAGER_CLIENTPORT,
        Property.TSERV_CLIENTPORT, Property.GC_CYCLE_START, Property.GC_CYCLE_DELAY,
        Property.GC_PORT, Property.MONITOR_PORT, Property.TABLE_MAJC_RATIO,
        Property.TABLE_SPLIT_THRESHOLD);
    for (var reqProp : requiredProps) {
      var confPropVal = config.get(reqProp);
      // already checked that all set properties are valid, just check that it is set then we know
      // it's valid
      if (confPropVal == null || confPropVal.isEmpty()) {
        log.warn("Required property {} is not set!", reqProp);
        status = Admin.CheckCommand.CheckStatus.FAILED;
      }
    }

    printCompleted(status);
    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}
