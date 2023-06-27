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
package org.apache.accumulo.core.util;

import java.security.SecureRandom;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.gson.Gson;

/**
 * This class provides easy access to global, immutable, lazily-instantiated, and thread-safe
 * singleton resources. These should be used with static imports.
 */
public class LazySingletons {

  // prevent instantiating this utility class
  private LazySingletons() {}

  /**
   * A Gson instance constructed with defaults. Construct your own if you need custom settings.
   */
  public static final Supplier<Gson> GSON = Suppliers.memoize(Gson::new);

  /**
   * A SecureRandom instance created with the default constructor.
   */
  public static final Supplier<SecureRandom> SECURE_RANDOM = Suppliers.memoize(SecureRandom::new);

}
