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
package org.apache.accumulo.server.fs;

import java.util.Set;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @deprecated since 2.1.0; implement {@link org.apache.accumulo.core.spi.fs.VolumeChooser} instead.
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_INTERFACE",
    justification = "Same name used for compatibility during deprecation cycle")
public interface VolumeChooser extends org.apache.accumulo.core.spi.fs.VolumeChooser {

  /**
   * Choose a volume from the provided options.
   *
   * @param env the server environment provided by the calling framework
   * @param options the list of volumes to choose from
   * @return one of the options
   * @throws VolumeChooserException if there is an error choosing (this is a RuntimeException); this
   *         does not preclude other RuntimeExceptions from occurring
   */
  default String choose(VolumeChooserEnvironment env, String[] options)
      throws VolumeChooserException {
    throw new UnsupportedOperationException("This method will be removed in 3.0");
  }

  /**
   * Default method provided for compatibility with 2.0.0.
   *
   * @since 2.1.0
   */
  @Override
  default String choose(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
      Set<String> options) {
    InterfaceEvolutionWarner.warnOnce(getClass(), VolumeChooser.class,
        "choose(VolumeChooserEnvironment,Set)", "3.0");
    return choose((VolumeChooserEnvironmentImpl) env, options.toArray(new String[0]));
  }

  /**
   * Default method provided for compatibility with 2.0.0.
   *
   * @since 2.1.0
   */
  @Override
  default Set<String> choosable(org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment env,
      Set<String> options) {
    // assume that all options are possible to be chosen by this chooser
    return options;
  }

  @Deprecated(since = "2.1.0")
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
