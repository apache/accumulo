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
package org.apache.accumulo.server.manager.state;

public enum MergeState {
  /**
   * Not merging
   */
  NONE,
  /**
   * created, stored in zookeeper, other merges are prevented on the table
   */
  STARTED,
  /**
   * put all matching tablets online, split tablets if we are deleting
   */
  SPLITTING,
  /**
   * after the tablet server chops the file, it marks the metadata table with a chopped marker
   */
  WAITING_FOR_CHOPPED,
  /**
   * when the number of chopped tablets in the range matches the number of online tablets in the
   * range, take the tablets offline
   */
  WAITING_FOR_OFFLINE,
  /**
   * when the number of chopped, offline tablets equals the number of merge tablets, begin the
   * metadata updates
   */
  MERGING,
  /**
   * merge is complete, the resulting tablet can be brought online, remove the marker in zookeeper
   */
  COMPLETE

}
