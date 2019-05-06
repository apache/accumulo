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
package org.apache.accumulo.monitor.rest.query;

import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX;
import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX_TABLE_ID;

import java.util.Map;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.Monitor.ScanStats;

/**
 * Generate a new Scan list JSON object
 *
 * @since 2.0.0
 */
@Path("/query/{tableName}/{value}")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class QueryResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates a new JSON object with scan information
   *
   * @return Scan JSON object
   */
  @GET
  public Query runQuery(@PathParam("tableName") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String tableName,
                        @PathParam("value") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String row) {
    return monitor.fetchQuery(tableName, new Range(row));
  }
}
