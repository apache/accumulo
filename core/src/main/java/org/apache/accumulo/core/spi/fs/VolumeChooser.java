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
 * Helper used to select a volume from a set of Volume URIs.
 * <p>
 * Volumes will be selected based on defined option criteria. Note: Implementations must be
 * threadsafe.<br>
 * VolumeChooser.equals will be used for internal caching.<br>
 * <p>
 * Property Details:<br>
 * {@link Property#GENERAL_ARBITRARY_PROP_PREFIX} and {@link Property#TABLE_ARBITRARY_PROP_PREFIX}
 * can be used to define user-specific properties to ensure separation from Accumulo System
 * defaults.<br>
 *
 * Note: The default VolumeChooser implementation is set by {@link Property#GENERAL_VOLUME_CHOOSER}.
 *
 * @since 2.1.0
 */
public interface VolumeChooser {

  /**
   * Choose a volume from the provided options.
   *
   * @param env the server environment provided by the calling framework
   * @param options the list of volumes to choose from
   * @return a volume from the list of volume options
   */
  String choose(VolumeChooserEnvironment env, Set<String> options);

  /**
   * Return the subset of all possible volumes that could be chosen across all invocations of
   * {@link #choose(VolumeChooserEnvironment, Set)}.<br>
   *
   * This is currently used to determine if the chosen volumes can support the required filesystem
   * operations for write ahead logs.<br>
   *
   * There may be other use cases in the future.
   */
  Set<String> choosable(VolumeChooserEnvironment env, Set<String> options);
}
