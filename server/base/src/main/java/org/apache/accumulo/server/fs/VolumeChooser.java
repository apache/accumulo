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
package org.apache.accumulo.server.fs;

import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.volume.Volume;

/**
 * Helper used by {@link VolumeManager}s to select from a set of {@link Volume} URIs. N.B.
 * implementations must be threadsafe. VolumeChooser.equals will be used for internal caching.
 *
 * <p>
 * Implementations may wish to store configuration in Accumulo's system configuration using the
 * {@link Property#GENERAL_ARBITRARY_PROP_PREFIX}. They may also benefit from using per-table
 * configuration using {@link Property#TABLE_ARBITRARY_PROP_PREFIX}.
 */
public interface VolumeChooser {

  /**
   * Choose a volume from the provided options.
   *
   * @param env
   *          the server environment provided by the calling framework
   * @param options
   *          the list of volumes to choose from
   * @return one of the options
   * @throws VolumeChooserException
   *           if there is an error choosing (this is a RuntimeException); this does not preclude
   *           other RuntimeExceptions from occurring
   * @deprecated since 2.1.0; override {@link #choose(VolumeChooserEnvironment, Set)} instead. This
   *             method will be removed in 3.0
   */
  @Deprecated
  default String choose(VolumeChooserEnvironment env, String[] options)
      throws VolumeChooserException {
    throw new UnsupportedOperationException("This method will be removed in 3.0");
  }

  /**
   * Choose a volume from the provided options.
   *
   * @param env
   *          the server environment provided by the calling framework
   * @param options
   *          the list of volumes to choose from
   * @return one of the options
   * @throws VolumeChooserException
   *           if there is an error choosing (this is a RuntimeException); this does not preclude
   *           other RuntimeExceptions from occurring
   */
  default String choose(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    InterfaceEvolutionWarner.warnOnce(getClass(), VolumeChooser.class,
        "choose(VolumeChooserEnvironment,Set)", "3.0");
    return choose(env, options.toArray(new String[0]));
  }

  /**
   * Return the subset of volumes that could possibly be chosen by this chooser across all
   * invocations of {@link #choose(VolumeChooserEnvironment, Set)}.
   *
   * @param env
   *          the server environment provided by the calling framework
   * @param options
   *          the subset of volumes to choose from
   * @return array of valid options
   * @throws VolumeChooserException
   *           if there is an error choosing (this is a RuntimeException); this does not preclude
   *           other RuntimeExceptions from occurring
   *
   * @since 2.1.0
   */
  default Set<String> choosable(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    // assume that all options are possible to be chosen by this chooser
    return options;
  }

  class VolumeChooserException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public VolumeChooserException(String message) {
      super(message);
    }

    public VolumeChooserException(String message, Throwable cause) {
      super(message, cause);
    }

  }

}
