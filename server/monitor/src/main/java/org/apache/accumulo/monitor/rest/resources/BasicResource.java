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
package org.apache.accumulo.monitor.rest.resources;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/rest")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class BasicResource {

  @Path("/master")
  public MasterResource getMasterResource() {

    return new MasterResource();
  }

  @Path("/bulkImports")
  public BulkImportResource getBulkImportResource() {

    return new BulkImportResource();
  }

  @Path("/gc")
  public GarbageCollectorResource getGarbageCollectorResource() {

    return new GarbageCollectorResource();
  }

  @Path("/logs")
  public LogResource getLogResource() {

    return new LogResource();
  }

  @Path("/problems")
  public ProblemsResource getProblemsResource() {

    return new ProblemsResource();
  }

  @Path("/replication")
  public ReplicationResource getReplicationResource() {

    return new ReplicationResource();
  }

  @Path("/scans")
  public ScansResource getScansResource() {

    return new ScansResource();
  }

  @Path("/statistics")
  public StatisticsResource getStatisticsResource() {

    return new StatisticsResource();
  }

  @Path("/tables")
  public TablesResource getTablesResource() {

    return new TablesResource();
  }

  @Path("/{parameter: tservers|json}")
  public TabletServerResource getTabletServerResource() {

    return new TabletServerResource();
  }

  @Path("/trace")
  public TracesResource getTracesResource() {

    return new TracesResource();
  }

  @Path("/zk")
  public ZookeeperResource getZookeeperResource() {

    return new ZookeeperResource();
  }
}
