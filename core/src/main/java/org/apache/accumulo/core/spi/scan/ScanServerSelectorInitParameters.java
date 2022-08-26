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
package org.apache.accumulo.core.spi.scan;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * This interface exists so that is easier to evolve what is passed to
 * {@link ScanServerSelector#init(ScanServerSelectorInitParameters)} without having to make breaking
 * changes.
 *
 * @since 2.1.0
 */
public interface ScanServerSelectorInitParameters {

  /**
   * @return Options that were set in the client config using the prefix
   *         {@code scan.server.selector.opts.}. The prefix will be stripped. For example if
   *         {@code scan.server.selector.opts.k1=v1} is set in client config, then the returned map
   *         will contain {@code k1=v1}.
   */
  Map<String,String> getOptions();

  ServiceEnvironment getServiceEnv();

  /**
   * @return the set of live ScanServers. Each time the supplier is called it may return something
   *         different. A good practice would be to call this no more than once per a call to
   *         {@link ScanServerSelector#determineActions(ScanServerSelectorParameters)} so that
   *         decisions are made using a consistent set of scan servers.
   */
  Supplier<Collection<ScanServerInfo>> getScanServers();

}
