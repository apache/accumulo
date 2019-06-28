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
package org.apache.accumulo.core.spi.security;


/**
 * A pluggable security module. The {@link #initialize(String, byte[])} method resets all the
 * security for Accumulo. The {@link #auth()} method returns the authorization and authentication
 * module.
 *
 * @since 2.1
 */
public interface SecurityModule {

  /**
   * Initialize the security for Accumulo. WARNING: Calling this will drop all users for Accumulo
   * and reset security. This is automatically called when Accumulo is initialized.
   */
  default void initialize(String rootUser, byte[] token) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Return the implemented {@link Auth} module for this SecurityModule.
   *
   * @return Auth
   */
  default Auth auth() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   *
   * @return
   */
  default Policy policy() {
    throw new UnsupportedOperationException("Not implemented");
  }

}
