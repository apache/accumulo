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

import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

/**
 * A per table scan dispatcher that decides which executor should be used to process a scan. For
 * information about configuring, find the documentation for the {@code table.scan.dispatcher} and
 * {@code table.scan.dispatcher.opts.} properties.
 *
 * @since 2.0.0
 * @see org.apache.accumulo.core.spi
 */
public interface ScanDispatcher {

  /**
   * The method parameters for {@link ScanDispatcher#init(InitParameters)}. This interface exists so
   * the API can evolve and additional parameters can be passed to the method in the future.
   *
   * @since 2.0.0
   */
  public interface InitParameters {
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
  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  /**
   * The method parameters for {@link ScanDispatcher#dispatch(DispatchParmaters)}. This interface
   * exists so the API can evolve and additional parameters can be passed to the method in the
   * future.
   *
   * @since 2.0.0
   * @deprecated since 2.1.0 replaced by {@link DispatchParameters} and
   *             {@link ScanDispatcher#dispatch(DispatchParameters)}
   */
  @Deprecated(since = "2.1.0")
  public interface DispatchParmaters extends DispatchParameters {}

  /**
   * @return Should return one of the executors named params.getScanExecutors().keySet()
   *
   * @deprecated since 2.1.0 please implement {@link #dispatch(DispatchParameters)} instead of this.
   *             Accumulo will only call {@link #dispatch(DispatchParameters)} directly, it will
   *             never call this. However the default implementation of
   *             {@link #dispatch(DispatchParameters)} calls this method.
   */
  @Deprecated(since = "2.1.0")
  default String dispatch(DispatchParmaters params) {
    throw new UnsupportedOperationException();
  }

  /**
   * The method parameters for {@link ScanDispatcher#dispatch(DispatchParameters)}. This interface
   * exists so the API can evolve and additional parameters can be passed to the method in the
   * future.
   *
   * @since 2.1.0
   */
  public interface DispatchParameters {
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
   * Accumulo calls this method for each scan batch to determine what executor to use and how to
   * utilize cache for the scan.
   *
   * @since 2.1.0
   */

  default ScanDispatch dispatch(DispatchParameters params) {
    String executor = dispatch((DispatchParmaters) params);
    if (executor.equals(DefaultScanDispatch.DEFAULT_SCAN_DISPATCH.getExecutorName())) {
      return DefaultScanDispatch.DEFAULT_SCAN_DISPATCH;
    }

    return ScanDispatch.builder().setExecutorName(executor).build();
  }
}
