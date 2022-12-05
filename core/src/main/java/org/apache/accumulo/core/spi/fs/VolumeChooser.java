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
package org.apache.accumulo.core.spi.fs;

import java.util.Set;

import org.apache.accumulo.core.conf.Property;

/**
 * Helper used to select from a set of Volume URIs. N.B. implementations must be threadsafe.
 * VolumeChooser.equals will be used for internal caching.
 *
 * <p>
 * Implementations may wish to store configuration in Accumulo's system configuration using the
 * {@link Property#GENERAL_ARBITRARY_PROP_PREFIX}. They may also benefit from using per-table
 * configuration using {@link Property#TABLE_ARBITRARY_PROP_PREFIX}.
 *
 * @since 2.1.0
 */
public interface VolumeChooser {

  /**
   * Choose a volume from the provided options.
   *
   * @param env the server environment provided by the calling framework
   * @param options the list of volumes to choose from
   * @return one of the options
   */
  String choose(VolumeChooserEnvironment env, Set<String> options);

  /**
   * Return the subset of volumes that could possibly be chosen by this chooser across all
   * invocations of {@link #choose(VolumeChooserEnvironment, Set)}. Currently this is used to
   * determine if all of the volumes that could be chosen for write ahead logs support the needed
   * filesystem operations. There may be other use cases in the future.
   *
   * @param env the server environment provided by the calling framework
   * @param options the subset of volumes to choose from
   * @return array of valid options
   */
  Set<String> choosable(VolumeChooserEnvironment env, Set<String> options);
}
