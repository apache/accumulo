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
package org.apache.accumulo.core.spi.scan;

import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

/**
 * A per table scan dispatcher that decides which executor should be used to processes a scan. For
 * information about configuring, find the documentation for the {@code table.scan.dispatcher} and
 * {@code table.scan.dispatcher.opts.} properties.
 *
 * @since 2.0.0
 */
public interface ScanDispatcher {

  /**
   * The method parameters for {@link ScanDispatcher#init(InitParameters)}. This interface exists so
   * the API can evolve and additional parameters can be passed to the method in the future.
   *
   * @since 2.0.0
   */
  public static interface InitParameters {
    /**
     *
     * @return The configured options. For example if the table properties
     *         {@code table.scan.dispatcher.opts.p1=abc} and
     *         {@code table.scan.dispatcher.opts.p9=123} were set, then this map would contain
     *         {@code p1=abc} and {@code p9=123}.
     */
    Map<String,String> getOptions();

    TableId getTableId();

    ServiceEnvironment getServiceEnv();
  }

  /**
   * This method is called once after a ScanDispatcher is instantiated.
   */
  public default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  /**
   * The method parameters for {@link ScanDispatcher#dispatch(DispatchParmaters)}. This interface
   * exists so the API can evolve and additional parameters can be passed to the method in the
   * future.
   *
   * @since 2.0.0
   */
  public static interface DispatchParmaters {
    /**
     * @return information about the scan to be dispatched.
     */
    ScanInfo getScanInfo();

    /**
     * @return the currently configured scan executors
     */
    Map<String,ScanExecutor> getScanExecutors();

    ServiceEnvironment getServiceEnv();
  }

  /**
   * @return Should return one of the executors named params.getScanExecutors().keySet()
   */
  String dispatch(DispatchParmaters params);
}
