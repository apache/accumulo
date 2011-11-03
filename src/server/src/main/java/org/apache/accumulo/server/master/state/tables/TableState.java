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
package org.apache.accumulo.server.master.state.tables;

public enum TableState {
  // NEW while making directories and tablets;
  // NEW transitions to LOADING
  NEW,
  
  // LOADING if tablets need assigned, and during assignment;
  // LOADING transitions to DISABLING, UNLOADING, DELETING, ONLINE
  LOADING,
  // ONLINE when all tablets are assigned (or suspected to be);
  // ONLINE transitions to LOADING, DISABLING, UNLOADING, DELETING
  ONLINE,
  
  // DISABLING when unloading tablets for DISABLED;
  // DISABLING transitions to DISABLED
  DISABLING,
  // DISABLED is like OFFLINE but won't come up on startup;
  // DISABLED transitions to LOADING, DELETING
  DISABLED,
  
  // UNLOADING when unloading tablets for OFFLINE;
  // UNLOADING transitions to OFFLINE
  UNLOADING,
  // OFFLINE is offline but will come up on startup (unless in safemode);
  // OFFLINE transitions to DISABLED, DELETING, LOADING (only in safemode)
  OFFLINE,
  
  // DELETING when unloading tablets, and cleaning up filesystem and metadata;
  // DELETING transitions to nothingness
  DELETING,
  
  // UNKNOWN is NOT a valid state; it is reserved for unrecognized serialized
  // representations of table state
  UNKNOWN;
}
