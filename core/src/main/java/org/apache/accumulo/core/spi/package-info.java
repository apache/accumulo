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
/**
 * This package provides a place for plugin interfaces intended for Accumulo users to implement. The
 * types under this package should adhere to the following rules.
 *
 * <ul>
 * <li>No changes should be made in a bug fix release.
 * <li>Any changes can be made in a minor or major version. Incompatible changes should only be made
 * if there is a benefit to users that outweighs the negative impact to users. If possible use
 * deprecation instead of making incompatible changes.
 * <li>All types used within this package should be declared in Accumulo's public API or under this
 * package. This rule makes it possible to achieve the other rules. Accumulo's build uses Apilyzer
 * to check this rule.
 * <li>Types under this package are intended for Accumulo users. If a type is only intended to be
 * used internally by Accumulo, it should not be placed here.
 * </ul>
 *
 * <p>
 * There are no hard and fast rules for a developer trying to decide if something should go into
 * this package, Accumulo's public API, or outside of both. If it can follow the rules then its
 * eligible for placement here. Below are some reasons things have or have not been placed here in
 * the past.
 *
 * <ul>
 * <li>Scan executors and cache plugins were placed here because they are tightly coupled to
 * Accumulo's scan execution model. If the execution model for scans is changed, incompatible
 * changes may have to be made. Trying to support a deprecation cycle may mean having to support a
 * new and old scan execution model in a single release, which may be impractical. Also these
 * plugins never impact users data or query results, they only impact performance via table
 * configuration.
 * <li>Crypto was placed here because its experimental and subject to change.
 * <li>Iterators are server side plugins, but were placed into Accumulo's public API instead of here
 * because they are so tightly coupled to users data model. Iterators can change the data returned
 * by a scan. The stricter rules of the API respect this tight coupling with users data model.
 * </ul>
 *
 * <p>
 * Before this package was created many plugin interface were created for Accumulo. These plugin
 * interfaces used internal Accumulo types, which transitively used other internal types. This
 * undisciplined use of any types made it impractical to reason about, analyze, or make any
 * guarantees about plugin stability. This package was created to solve that problem. Hopefully
 * existing plugins (like the balancer) can be migrated to this package.
 */
package org.apache.accumulo.core.spi;
