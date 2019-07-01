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
package org.apache.accumulo.server.security.handler;

//TODO Move this into SPI once ready.
/**
 * A pluggable security module. The {@link #initialize(String, byte[])} method resets all the
 * security for Accumulo. The {@link #auth()} method returns the authorization and authentication
 * module. The {@link #perm()} method returns the permissions module.
 *
 * @since 2.1
 */
public interface SecurityModule {

  /**
   * Initialize the security for Accumulo. WARNING: Calling this will drop all users for Accumulo
   * and reset security. This is automatically called when Accumulo is initialized.
   */
  void initialize(String rootUser, byte[] token);

  /**
   * Return the implemented {@link Auth} module for this SecurityModule.
   *
   * @return Auth
   */
  Auth auth();

  /**
   * Return the implemented {@link Perm} module for this SecurityModule.
   *
   * @return Perm
   */
  Perm perm();

}
