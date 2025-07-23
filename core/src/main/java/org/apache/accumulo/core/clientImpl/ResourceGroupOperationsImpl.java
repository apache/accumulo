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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ResourceGroupNotFoundException;
import org.apache.accumulo.core.client.admin.ResourceGroupOperations;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.clientImpl.thrift.ThriftResourceGroupNotExistsException;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.Retry;
import org.slf4j.LoggerFactory;

public class ResourceGroupOperationsImpl implements ResourceGroupOperations {

  private final ClientContext context;

  public ResourceGroupOperationsImpl(ClientContext context) {
    checkArgument(context != null, "context is null");
    this.context = context;
  }

  @Override
  public boolean exists(String group) {
    Objects.requireNonNull(group, "group parameter must be supplied");
    return list().contains(ResourceGroupId.of(group));
  }

  @Override
  public Set<ResourceGroupId> list() {
    Set<ResourceGroupId> groups = new HashSet<>();
    context.getZooCache().getChildren(Constants.ZRESOURCEGROUPS)
        .forEach(c -> groups.add(ResourceGroupId.of(c)));
    return Set.copyOf(groups);
  }

  @Override
  public void create(ResourceGroupId group) throws AccumuloException, AccumuloSecurityException {
    ThriftClientTypes.MANAGER.executeVoid(context, client -> client
        .createResourceGroupNode(TraceUtil.traceInfo(), context.rpcCreds(), group.canonical()));
  }

  @Override
  public Map<String,String> getConfiguration(ResourceGroupId group)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    Map<String,String> config = new HashMap<>();
    config.putAll(ThriftClientTypes.CLIENT.execute(context, client -> client
        .getConfiguration(TraceUtil.traceInfo(), context.rpcCreds(), ConfigurationType.CURRENT)));
    config.putAll(getProperties(group));
    return Map.copyOf(config);
  }

  @Override
  public Map<String,String> getProperties(ResourceGroupId group)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    try {
      TVersionedProperties rgProps = ThriftClientTypes.CLIENT.execute(context,
          client -> client.getVersionedResourceGroupProperties(TraceUtil.traceInfo(),
              context.rpcCreds(), group.canonical()));
      if (rgProps != null && rgProps.getPropertiesSize() > 0) {
        return Map.copyOf(rgProps.getProperties());
      } else {
        return Map.of();
      }
    } catch (AccumuloException | AccumuloSecurityException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftResourceGroupNotExistsException) {
        ThriftResourceGroupNotExistsException te = (ThriftResourceGroupNotExistsException) t;
        throw new ResourceGroupNotFoundException(te.getResourceGroupName());
      }
      throw e;
    }
  }

  @Override
  public void setProperty(ResourceGroupId group, String property, String value)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    checkArgument(property != null, "property is null");
    checkArgument(value != null, "value is null");
    DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {
      // force a warning on the client side, but send the name the user used to the server-side
      // to trigger a warning in the server logs, and to handle it there
      log.warn("{} was deprecated and will be removed in a future release;"
          + " setting its replacement {} instead", property, replacement);
    });
    try {
      ThriftClientTypes.MANAGER.executeVoid(context,
          client -> client.setResourceGroupProperty(TraceUtil.traceInfo(), context.rpcCreds(),
              group.canonical(), property, value));
    } catch (AccumuloException | AccumuloSecurityException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftResourceGroupNotExistsException) {
        ThriftResourceGroupNotExistsException te = (ThriftResourceGroupNotExistsException) t;
        throw new ResourceGroupNotFoundException(te.getResourceGroupName());
      }
      throw e;
    }
    checkLocalityGroups(property);
  }

  private Map<String,String> tryToModifyProperties(final ResourceGroupId group,
      final Consumer<Map<String,String>> mapMutator) throws AccumuloException,
      AccumuloSecurityException, IllegalArgumentException, ResourceGroupNotFoundException {
    checkArgument(mapMutator != null, "mapMutator is null");

    TVersionedProperties vProperties;
    try {
      vProperties = ThriftClientTypes.CLIENT.execute(context,
          client -> client.getVersionedResourceGroupProperties(TraceUtil.traceInfo(),
              context.rpcCreds(), group.canonical()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftResourceGroupNotExistsException) {
        ThriftResourceGroupNotExistsException te = (ThriftResourceGroupNotExistsException) t;
        throw new ResourceGroupNotFoundException(te.getResourceGroupName());
      }
      throw e;
    }
    mapMutator.accept(vProperties.getProperties());

    // A reference to the map was passed to the user, maybe they still have the reference and are
    // modifying it. Buggy Accumulo code could attempt to make modifications to the map after this
    // point. Because of these potential issues, create an immutable snapshot of the map so that
    // from here on the code is assured to always be dealing with the same map.
    vProperties.setProperties(Map.copyOf(vProperties.getProperties()));

    for (Map.Entry<String,String> entry : vProperties.getProperties().entrySet()) {
      final String property = Objects.requireNonNull(entry.getKey(), "property key is null");

      DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {
        // force a warning on the client side, but send the name the user used to the
        // server-side
        // to trigger a warning in the server logs, and to handle it there
        log.warn("{} was deprecated and will be removed in a future release;"
            + " setting its replacement {} instead", property, replacement);
      });
      checkLocalityGroups(property);
    }

    // Send to server
    try {
      ThriftClientTypes.MANAGER.executeVoid(context,
          client -> client.modifyResourceGroupProperties(TraceUtil.traceInfo(), context.rpcCreds(),
              group.canonical(), vProperties));
    } catch (AccumuloException | AccumuloSecurityException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftResourceGroupNotExistsException) {
        ThriftResourceGroupNotExistsException te = (ThriftResourceGroupNotExistsException) t;
        throw new ResourceGroupNotFoundException(te.getResourceGroupName());
      }
      throw e;
    }

    return vProperties.getProperties();
  }

  @Override
  public Map<String,String> modifyProperties(ResourceGroupId group,
      Consumer<Map<String,String>> mapMutator) throws AccumuloException, AccumuloSecurityException,
      IllegalArgumentException, ResourceGroupNotFoundException {

    var log = LoggerFactory.getLogger(InstanceOperationsImpl.class);

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(25))
        .incrementBy(Duration.ofMillis(25)).maxWait(Duration.ofSeconds(30)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (true) {
      try {
        var props = tryToModifyProperties(group, mapMutator);
        retry.logCompletion(log, "Modifying resource group properties");
        return props;
      } catch (ConcurrentModificationException cme) {
        try {
          retry.logRetry(log,
              "Unable to modify resource group properties for because of concurrent modification");
          retry.waitForNextAttempt(log, "Modify resource group properties");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } finally {
        retry.useRetry();
      }
    }
  }

  @Override
  public void removeProperty(ResourceGroupId group, String property)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    checkArgument(property != null, "property is null");
    DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {
      // force a warning on the client side, but send the name the user used to the server-side
      // to trigger a warning in the server logs, and to handle it there
      log.warn("{} was deprecated and will be removed in a future release; assuming user meant"
          + " its replacement {} and will remove that instead", property, replacement);
    });
    try {
      ThriftClientTypes.MANAGER.executeVoid(context,
          client -> client.removeResourceGroupProperty(TraceUtil.traceInfo(), context.rpcCreds(),
              group.canonical(), property));
    } catch (AccumuloException | AccumuloSecurityException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftResourceGroupNotExistsException) {
        ThriftResourceGroupNotExistsException te = (ThriftResourceGroupNotExistsException) t;
        throw new ResourceGroupNotFoundException(te.getResourceGroupName());
      }
      throw e;
    }
    checkLocalityGroups(property);
  }

  @Override
  public void remove(ResourceGroupId group)
      throws AccumuloException, AccumuloSecurityException, ResourceGroupNotFoundException {
    try {
      ThriftClientTypes.MANAGER.executeVoid(context, client -> client
          .removeResourceGroupNode(TraceUtil.traceInfo(), context.rpcCreds(), group.canonical()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftResourceGroupNotExistsException) {
        ThriftResourceGroupNotExistsException te = (ThriftResourceGroupNotExistsException) t;
        throw new ResourceGroupNotFoundException(te.getResourceGroupName());
      }
      throw e;
    }
  }

  private void checkLocalityGroups(String propChanged)
      throws AccumuloSecurityException, AccumuloException {
    if (LocalityGroupUtil.isLocalityGroupProperty(propChanged)) {
      try {
        LocalityGroupUtil
            .checkLocalityGroups(context.instanceOperations().getSystemConfiguration());
      } catch (LocalityGroupConfigurationError | RuntimeException e) {
        LoggerFactory.getLogger(this.getClass()).warn("Changing '" + propChanged
            + "' resulted in bad locality group config. This may be a transient situation since "
            + "the config spreads over multiple properties. Setting properties in a different "
            + "order may help. Even though this warning was displayed, the property was updated. "
            + "Please check your config to ensure consistency.", e);
      }
    }
  }
}
