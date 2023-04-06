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
package org.apache.accumulo.core.client.sample;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

/**
 * A function that decides which key values are stored in a tables sample. As Accumulo compacts data
 * and creates rfiles it uses a Sampler to decided what to store in the rfiles sample section. The
 * class name of the Sampler and the Samplers configuration are stored in each rfile. A scan of a
 * tables sample will only succeed if all rfiles were created with the same sampler and sampler
 * configuration.
 *
 * <p>
 * Since the decisions that Sampler makes are persisted, the behavior of a Sampler for a given
 * configuration should always be the same. One way to offer a new behavior is to offer new options,
 * while still supporting old behavior with a Samplers existing options.
 *
 * <p>
 * Ideally a sampler that selects a Key k1 would also select updates for k1. For example if a
 * Sampler selects :
 * {@code row='000989' family='name' qualifier='last' visibility='ADMIN' time=9 value='Doe'}, it
 * would be nice if it also selected :
 * {@code row='000989' family='name' qualifier='last' visibility='ADMIN' time=20 value='Dough'}.
 * Using hash and modulo on the key fields is a good way to accomplish this and
 * {@link AbstractHashSampler} provides a good basis for implementation.
 *
 * @since 1.8.0
 */
public interface Sampler {

  /**
   * An implementation of Sampler must have a noarg constructor. After construction this method is
   * called once to initialize a sampler before it is used.
   *
   * @param config Configuration options for a sampler.
   */
  void init(SamplerConfiguration config);

  /**
   * @param k A key that was written to a rfile.
   * @return True if the key (and its associated value) should be stored in the rfile's sample.
   *         Return false if it should not be included.
   */
  boolean accept(Key k);

  /**
   * @param config Sampler options configuration to validate. Validates option and value.
   */
  default void validateOptions(Map<String,String> config) {}
}
