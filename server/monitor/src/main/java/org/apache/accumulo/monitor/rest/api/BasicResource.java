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
package org.apache.accumulo.monitor.rest.api;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.monitor.rest.api.bulkImport.BulkImportResource;
import org.apache.accumulo.monitor.rest.api.garbageCollector.GarbageCollectorResource;
import org.apache.accumulo.monitor.rest.api.log.LogResource;
import org.apache.accumulo.monitor.rest.api.master.MasterResource;
import org.apache.accumulo.monitor.rest.api.problem.ProblemsResource;
import org.apache.accumulo.monitor.rest.api.replication.ReplicationResource;
import org.apache.accumulo.monitor.rest.api.scan.ScansResource;
import org.apache.accumulo.monitor.rest.api.statistic.StatisticsResource;
import org.apache.accumulo.monitor.rest.api.status.StatusResource;
import org.apache.accumulo.monitor.rest.api.table.TablesResource;
import org.apache.accumulo.monitor.rest.api.trace.TracesResource;
import org.apache.accumulo.monitor.rest.api.tserver.TabletServerResource;
import org.apache.accumulo.monitor.rest.api.zookeeper.ZookeeperResource;

/**
 * BasicResource is used to set the path for the REST calls
 *
 * @since 2.0.0
 *
 */
@Path("/rest")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class BasicResource {

  /**
   * Generates Master REST calls
   *
   * @return master information in JSON
   */
  @Path("/master")
  public MasterResource getMasterResource() {

    return new MasterResource();
  }

  /**
   * Generates Bulk Import REST calls
   *
   * @return bulk import information in JSON
   */
  @Path("/bulkImports")
  public BulkImportResource getBulkImportResource() {

    return new BulkImportResource();
  }

  /**
   * Generates Garbage Collector REST calls
   *
   * @return garbage collector information in JSON
   */
  @Path("/gc")
  public GarbageCollectorResource getGarbageCollectorResource() {

    return new GarbageCollectorResource();
  }

  /**
   * Generates Log REST calls
   *
   * @return log information in JSON
   */
  @Path("/logs")
  public LogResource getLogResource() {

    return new LogResource();
  }

  /**
   * Generates Problem REST calls
   *
   * @return problem information in JSON
   */
  @Path("/problems")
  public ProblemsResource getProblemsResource() {

    return new ProblemsResource();
  }

  /**
   * Generates Replication REST calls
   *
   * @return replication information in JSON
   */
  @Path("/replication")
  public ReplicationResource getReplicationResource() {

    return new ReplicationResource();
  }

  /**
   * Generates Scan REST calls
   *
   * @return scan information in JSON
   */
  @Path("/scans")
  public ScansResource getScansResource() {

    return new ScansResource();
  }

  /**
   * Generates Statistics REST calls
   *
   * @return statistic information in JSON
   */
  @Path("/statistics")
  public StatisticsResource getStatisticsResource() {

    return new StatisticsResource();
  }

  /**
   * Generates Status REST calls
   *
   * @return status information in JSON
   */
  @Path("/status")
  public StatusResource getStatusResource() {

    return new StatusResource();
  }

  /**
   * Generates Table REST calls
   *
   * @return table information in JSON
   */
  @Path("/tables")
  public TablesResource getTablesResource() {

    return new TablesResource();
  }

  /**
   * Generates TServer REST calls and JSON object
   *
   * @return tserver information in JSON
   */
  @Path("/{parameter: tservers|json}")
  public TabletServerResource getTabletServerResource() {

    return new TabletServerResource();
  }

  /**
   * Generates Trace REST calls
   *
   * @return trace information in JSON
   */
  @Path("/trace")
  public TracesResource getTracesResource() {

    return new TracesResource();
  }

  /**
   * Generates Zookeeper REST calls
   *
   * @return zookeeper information in JSON
   */
  @Path("/zk")
  public ZookeeperResource getZookeeperResource() {

    return new ZookeeperResource();
  }
}
