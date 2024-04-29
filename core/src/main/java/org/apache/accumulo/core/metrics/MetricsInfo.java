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

import org.apache.accumulo.core.util.HostAndPort;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

public interface MetricsInfo {

  /**
   * Convenience method to create tag name / value pair for the instance name
   *
   * @param instanceName the instance name
   */
  static Tag instanceNameTag(final String instanceName) {
    Objects.requireNonNull(instanceName,
        "cannot create the tag without providing the instance name");
    return Tag.of("instance.name", instanceName);
  }

  /**
   * Convenience method to create tag name / value pair for the process name
   *
   * @param processName the process name
   */
  static Tag processTag(final String processName) {
    Objects.requireNonNull(processName, "cannot create the tag without providing the process name");
    return Tag.of("process.name", processName);
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
    tags.add(Tag.of("host", hostAndPort.getHost()));
    int port = hostAndPort.getPort();
    if (port != 0) {
      tags.add(Tag.of("port", Integer.toString(hostAndPort.getPort())));
    }
    return Collections.unmodifiableList(tags);
  }

  boolean isMetricsEnabled();

  /**
   * Convenience method to set the common tags for application (process), host and port.
   *
   * @param applicationName the application (process) name.
   * @param hostAndPort the host:port pair
   */
  void addServiceTags(final String applicationName, final HostAndPort hostAndPort);

  /**
   * Add the list of tag name / value pair to the common tags that will be emitted with all metrics.
   * Common tags must ne added before initialization of any registries. Tags that are added after a
   * registry is initialized may not be emitted by the underlying metrics system. This would cause
   * inconsistent grouping and filtering based on tags,
   *
   * @param updates list of tags (name / value pairs)
   */
  void addCommonTags(final List<Tag> updates);

  /**
   * Get the current list of common tags.
   */
  Collection<Tag> getCommonTags();

  void addRegistry(MeterRegistry registry);

  void addMetricsProducers(MetricsProducer... producer);

  /**
   * Initialize the metrics system. This sets the list of common tags that are emitted with the
   * metrics.
   */
  void init();

  MeterRegistry getRegistry();

  /**
   * Close the underlying registry and release resources. The registry will not accept new meters
   * and will stop publishing metrics.
   */
  void close();
}
