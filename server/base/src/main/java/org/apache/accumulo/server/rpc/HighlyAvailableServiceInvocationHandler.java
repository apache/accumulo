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
package org.apache.accumulo.server.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.server.HighlyAvailableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link InvocationHandler} which checks to see if a {@link HighlyAvailableService} is the
 * current active instance of that service, throwing {@link ThriftNotActiveServiceException} when it
 * is not the current active instance.
 */
public class HighlyAvailableServiceInvocationHandler<I> implements InvocationHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(HighlyAvailableServiceInvocationHandler.class);

  private final I instance;
  private final HighlyAvailableService service;

  public HighlyAvailableServiceInvocationHandler(I instance, HighlyAvailableService service) {
    this.instance = Objects.requireNonNull(instance);
    this.service = Objects.requireNonNull(service);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

    // If the service is upgrading, throw an exception
    if (service.isUpgrading()) {
      LOG.trace("Service can not be accessed while it is upgrading.");
      throw new ThriftNotActiveServiceException(service.getServiceName(),
          "Service can not be accessed while it is upgrading");
    }

    // If the service is not active, throw an exception
    if (!service.isActiveService()) {
      LOG.trace("Denying access to RPC service as this instance is not the active instance.");
      throw new ThriftNotActiveServiceException(service.getServiceName(),
          "Denying access to RPC service as this instance is not the active instance");
    }
    try {
      // Otherwise, call the real method
      return method.invoke(instance, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }
}
