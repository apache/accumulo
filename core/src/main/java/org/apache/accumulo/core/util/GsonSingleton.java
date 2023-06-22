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

import com.google.gson.Gson;

/**
 * This class provides access to a shared instance of Gson that uses its default configuration. If you need to use a Gson instance that is configured differently, you want to configure TypeAdapters for example, then you should not use this. You should construct your own instance of Gson.
 * <p>
 * Gson is thread-safe, so it should be safe to create and reuse a single instance of Gson.
 * </p>
 */
public class GsonSingleton {
  private static Gson gsonInstance = null;

  private GsonSingleton() {
    // private to prevent direct instantiation
  }

  public static Gson getInstance() {
    if (gsonInstance == null) {
      gsonInstance = new Gson();
    }
    return gsonInstance;
  }
}
