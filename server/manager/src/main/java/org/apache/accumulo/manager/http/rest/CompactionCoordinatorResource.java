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
package org.apache.accumulo.manager.http.rest;

import java.util.Objects;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.rest.request.GetCompactionJobRequest;
import org.apache.thrift.TBase;
import org.apache.thrift.async.AsyncMethodCallback;

@Path("/cc")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class CompactionCoordinatorResource {

  @Inject
  private Manager manager;

  @POST
  @Path("get-compaction-job")
  @Consumes(MediaType.APPLICATION_JSON)
  public void getCompactionJob(@Suspended AsyncResponse response, GetCompactionJobRequest request)
      throws Exception {
    manager.getCompactionCoordinator().getCompactionJob(request.getTinfo(),
        request.getCredentials(), request.getGroupName(), request.getCompactorAddress(),
        request.getExternalCompactionId(), wrap(response));
  }

  // TODO: We can eventually create our own Async callback interface if we wanted, I just
  // reused the Thrift version for now as we are already using Thrift objects anyways.
  // AsyncMethodCallback is what async thrift uses
  private static <T extends TBase<?,?>> AsyncMethodCallback<T> wrap(AsyncResponse response) {
    return new JerseyAsyncWrapper<>(response);
  }

  private static class JerseyAsyncWrapper<T extends TBase<?,?>> implements AsyncMethodCallback<T> {
    private final AsyncResponse asyncResponse;

    private JerseyAsyncWrapper(AsyncResponse asyncResponse) {
      this.asyncResponse = Objects.requireNonNull(asyncResponse);
    }

    @Override
    public void onComplete(T response) {
      asyncResponse.resume(response);
    }

    @Override
    public void onError(Exception exception) {
      asyncResponse.resume(exception);
    }
  }

}
