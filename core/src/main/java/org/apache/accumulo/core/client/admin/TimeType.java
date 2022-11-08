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
package org.apache.accumulo.core.client.admin;

/**
 * The type of ordering to use for the table's entries (default is MILLIS)
 */
public enum TimeType {
  /**
   * Used to guarantee ordering of data sequentially as inserted
   */
  LOGICAL,

  /**
   * This is the default. Tries to ensure that inserted data is stored with the timestamp closest to
   * the machine's time to the nearest millisecond, without going backwards to guarantee insertion
   * sequence. Note that using this time type can cause time to "skip" forward if a machine has a
   * time that is too far off. NTP is recommended when using this type.
   */
  MILLIS
}
