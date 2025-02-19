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
package org.apache.accumulo.core.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.Tag;

public interface MetricsInfo {

  String INSTANCE_NAME_TAG_KEY = "instance.name";
  String PROCESS_NAME_TAG_KEY = "process.name";
  String RESOURCE_GROUP_TAG_KEY = "resource.group";
  String HOST_TAG_KEY = "host";
  String PORT_TAG_KEY = "port";

  Set<String> allTags = Set.of(INSTANCE_NAME_TAG_KEY, PROCESS_NAME_TAG_KEY, RESOURCE_GROUP_TAG_KEY,
      HOST_TAG_KEY, PORT_TAG_KEY);

  /**
   * Convenience method to create tag name / value pair for the instance name
   *
   * @param instanceName the instance name
   */
  static Tag instanceNameTag(final String instanceName) {
    Objects.requireNonNull(instanceName,
        "cannot create the tag without providing the instance name");
    return Tag.of(INSTANCE_NAME_TAG_KEY, instanceName);
  }

  /**
   * Convenience method to create tag name / value pair for the process name
   *
   * @param processName the process name
   */
  static Tag processTag(final String processName) {
    Objects.requireNonNull(processName, "cannot create the tag without providing the process name");
    return Tag.of(PROCESS_NAME_TAG_KEY, processName);
  }

  /**
   * Convenience method to create tag name / value pair for the resource group name
   *
   * @param resourceGroupName the resource group name
   */
  static Tag resourceGroupTag(final String resourceGroupName) {
    if (resourceGroupName == null || resourceGroupName.isEmpty()) {
      return Tag.of(RESOURCE_GROUP_TAG_KEY, "NOT_PROVIDED");
    }
    return Tag.of(RESOURCE_GROUP_TAG_KEY, resourceGroupName);
  }

  /**
   * Convenience method to create tag name / value pairs for the host and port from address
   * host:port pair.
   *
   * @param hostAndPort the host:port pair
   */
  static List<Tag> addressTags(final HostAndPort hostAndPort) {
    Objects.requireNonNull(hostAndPort, "cannot create the tag without providing the hostAndPort");
    List<Tag> tags = new ArrayList<>(2);
    tags.add(Tag.of(HOST_TAG_KEY, hostAndPort.getHost()));
    int port = hostAndPort.getPort();
    if (port != 0) {
      tags.add(Tag.of(PORT_TAG_KEY, Integer.toString(hostAndPort.getPort())));
    }
    return Collections.unmodifiableList(tags);
  }

  boolean isMetricsEnabled();

  /**
   * Common tags for all services.
   */
  static Collection<Tag> serviceTags(final String instanceName, final String applicationName,
      final HostAndPort hostAndPort, final String resourceGroupName) {
    List<Tag> tags = new ArrayList<>();
    tags.add(instanceNameTag(instanceName));
    tags.add(processTag(applicationName));
    tags.addAll(addressTags(hostAndPort));
    tags.add(resourceGroupTag(resourceGroupName));
    return tags;
  }

  void addMetricsProducers(MetricsProducer... producer);

  /**
   * Initialize the metrics system. This sets the list of common tags that are emitted with the
   * metrics.
   */
  void init(Collection<Tag> commonTags);

  /**
   * Close the underlying registry and release resources. The registry will not accept new meters
   * and will stop publishing metrics.
   */
  void close();
}
