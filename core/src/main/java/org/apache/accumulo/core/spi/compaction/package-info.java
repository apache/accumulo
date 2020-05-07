/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * This package provides a place for plugin interfaces related to executing compactions. The diagram
 * below shows the functional components in Accumulo related to compactions. Not all of these
 * components are pluggable, but understanding how everything fits together is important for writing
 * a plugin.
 *
 * <p>
 * <img src="doc-files/compaction-spi-design.png" alt="Compaction design diagram">
 *
 * <p>
 * The following is a desciption of each functional component.
 *
 * <ul>
 * <li><b>Compaction Manager</b> A non pluggable component within the tablet server that brings all
 * other components together. The manager will route compactables to compaction services. For each
 * kind of compaction, an individual compactible will be routed to a single compaction service. For
 * example its possible that compactable C1 is routed to service S1 for user compactions and service
 * S2 for system compactions.
 * <ul>
 * <li><b>Compaction Service</b> A non pluggable component that compacts tablets. One or more of
 * these are created based on user configuration. Users can assign a table to a compaction service.
 * Has a single compaction planner and one ore more compaction executors.
 * <ul>
 * <li><b>Compaction Planner</b> A pluggable component that can be configured by users when they
 * configure a compaction service. It makes decisions about which files to compact on which
 * executors. See {@link org.apache.accumulo.core.spi.compaction.CompactionPlanner},
 * {@link org.apache.accumulo.core.spi.compaction.CompactionPlanner#makePlan(org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters)},
 * and {@link org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner}
 * <li><b>Compaction Executor</b> A non pluggable component that executes compactions using multiple
 * threads and has a priority queue.</li>
 * </ul>
 * </li>
 * <li><b>Compactable</b> A non pluggable component that wraps a Tablet and per table pluggable
 * compaction components. It tracks all information about one or more running compactions that is
 * needed by a compaction service in a thread safe manor. There is a 1 to 1 relationship between
 * compactables and tablets.
 * <ul>
 * <li><b>Compaction Dispatcher</b> A pluggable component component that decides which compaction
 * service a table should use for different kinds of compactions. This is configurable by users per
 * table. See {@link org.apache.accumulo.core.spi.compaction.CompactionDispatcher}</li>
 * <li><b>Compaction Selector</b> A pluggable component that can optionally be configured per table
 * to periodically select files to compact. This supports use cases like periodically compacting all
 * files because there are too many deletes. See
 * {@link org.apache.accumulo.core.client.admin.compaction.CompactionSelector}</li>
 * <li><b>Compaction Configurer</b> A pluggable component that can optionally be configured per
 * table to dynamically configure file output settings. This supports use cases like using snappy
 * for small files and gzip for large files. See
 * {@link org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer}</li>
 * <li><b>Compaction Strategy</b> A deprecated pluggable component replaced by the Selector and
 * Configurer. See {@link org.apache.accumulo.core.client.admin.CompactionStrategyConfig} for more
 * information about why this was deprecated.
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 *
 *
 *
 * @see org.apache.accumulo.core.spi
 */
package org.apache.accumulo.core.spi.compaction;
