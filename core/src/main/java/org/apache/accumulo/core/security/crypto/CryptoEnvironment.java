/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.security.crypto;

import java.util.Map;

public class CryptoEnvironment {
  /**
   * Where in Accumulo the on-disk file encryption takes place.
   */
  public enum Scope {
    WAL, RFILE;
  }

  private Scope scope;
  private Map<String,String> conf;
  private String parameters;

  public CryptoEnvironment(Scope scope, Map<String,String> conf) {
    this.scope = scope;
    this.conf = conf;
  }

  public Scope getScope() {
    return this.scope;
  };

  public Map<String,String> getConf() {
    return this.conf;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }
}
