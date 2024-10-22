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
package org.apache.accumulo.monitor.rest.compactions.external;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.monitor.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate a new External compactions resource
 *
 * @since 2.1.0
 */
@Path("/ec")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class ECResource {
  private static final Logger log = LoggerFactory.getLogger(ECResource.class);

  @Inject
  private Monitor monitor;

  @GET
  public CoordinatorInfo getCoordinator() {
    var cc = monitor.getCompactorsInfo();
    log.info("Got coordinator from monitor = {}", cc.getCoordinatorHost());
    return new CoordinatorInfo(cc.getCoordinatorHost(), cc);
  }

  @Path("compactors")
  @GET
  public Compactors getCompactors() {
    return new Compactors(monitor.getCompactorsInfo());
  }

  @Path("running")
  @GET
  public RunningCompactions getRunning() {
    // return monitor.getRunnningCompactions();
    return getSampleData();
  }

  private RunningCompactions getSampleData() {

    RunningCompactionInfo rci1 =
        new RunningCompactionInfo("server1", "queue1", "ecid1", "USER", "tableId1", 150, 0.1f,
            Duration.ofMinutes(15).toNanos(), TCompactionState.ASSIGNED.name(), 10000);
    RunningCompactionInfo rci2 =
        new RunningCompactionInfo("server2", "queue2", "ecid2", "SYSTEM", "tableId2", 265, 0.2f,
            Duration.ofHours(2).toNanos(), TCompactionState.CANCELLED.name(), 20000);
    RunningCompactionInfo rci3 =
        new RunningCompactionInfo("server3", "queue3", "ecid3", "USER", "tableId3", 3555, 0.3f,
            Duration.ofMinutes(70).toNanos(), TCompactionState.IN_PROGRESS.name(), 30000);
    RunningCompactionInfo rci4 =
        new RunningCompactionInfo("localhost:9999", "default", "ecid4", "SYSTEM", "1", 132342, 0.1f,
            Duration.ofSeconds(15).toNanos(), TCompactionState.IN_PROGRESS.name(), 10000);

    List<RunningCompactionInfo> running = List.of(rci1, rci2, rci3, rci4);

    RunningCompactions result = new RunningCompactions(Map.of());
    result.running.addAll(running);

    return result;
  }

  @Path("details")
  @GET
  public RunningCompactorDetails getDetails(@QueryParam("ecid") @NotNull String ecid) {
    // make parameter more user-friendly by ensuring the ecid prefix is present
    ecid = ExternalCompactionId.from(ecid).canonical();
    var runningCompactorDetails =
        monitor.getRunningCompactorDetails(ExternalCompactionId.from(ecid));
    if (runningCompactorDetails == null) {
      throw new IllegalStateException("Failed to find details for ECID: " + ecid);
    }
    return runningCompactorDetails;
  }
}
