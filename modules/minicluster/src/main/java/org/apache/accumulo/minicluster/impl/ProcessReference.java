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

package org.apache.accumulo.minicluster.impl;

/**
 * Opaque handle to a process.
 */
public class ProcessReference {
  private Process process;

  ProcessReference(Process process) {
    this.process = process;
  }

  /**
   * Visible for testing, not intended for client consumption
   */
  public Process getProcess() {
    return process;
  }

  @Override
  public String toString() {
    return process.toString();
  }

  @Override
  public int hashCode() {
    return process.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Process) {
      return process == obj;
    }
    return this == obj;
  }
}
