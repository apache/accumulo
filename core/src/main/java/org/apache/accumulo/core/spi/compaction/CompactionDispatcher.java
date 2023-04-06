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
package org.apache.accumulo.core.spi.compaction;

import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

/**
 * Can be configured per table to dispatch compactions to different compaction services. For a given
 * table the dispatcher can choose a different compaction service for each kind of compaction. For
 * example, user and chop compactions could be dispatched to service_A while system compactions are
 * dispatched to service_B.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public interface CompactionDispatcher {
  /**
   * The method parameters for {@link CompactionDispatcher#init(InitParameters)}. This interface
   * exists so the API can evolve and additional parameters can be passed to the method in the
   * future.
   *
   * @since 2.1.0
   */
  public interface InitParameters {
    /**
     *
     * @return The configured options. For example if the table properties
     *         {@code table.compaction.dispatcher.opts.p1=abc} and
     *         {@code table.compaction.dispatcher.opts.p9=123} were set, then this map would contain
     *         {@code p1=abc} and {@code p9=123}.
     */
    Map<String,String> getOptions();

    TableId getTableId();

    ServiceEnvironment getServiceEnv();
  }

  /**
   * This method is called once after a CompactionDispatcher is instantiated.
   */
  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  /**
   * The method parameters for {@link CompactionDispatcher#dispatch(DispatchParameters)}. This
   * interface exists so the API can evolve and additional parameters can be passed to the method in
   * the future.
   *
   * @since 2.1.0
   */
  public interface DispatchParameters {
    /**
     * @return the currently configured compaction services
     */
    CompactionServices getCompactionServices();

    ServiceEnvironment getServiceEnv();

    CompactionKind getCompactionKind();

    Map<String,String> getExecutionHints();
  }

  /**
   * Accumulo calls this method for compactions to determine what service to use.
   */
  CompactionDispatch dispatch(DispatchParameters params);
}
