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
package org.apache.accumulo.tserver.metrics;

public interface TabletServerUpdateMetricsMBean {

  final static String permissionErrors = "permissionErrors";
  final static String unknownTabletErrors = "unknownTabletErrors";
  final static String mutationArraySize = "mutationArraysSize";
  final static String commitPrep = "commitPrep";
  final static String constraintViolations = "constraintViolations";
  final static String waLogWriteTime = "waLogWriteTime";
  final static String commitTime = "commitTime";

  long getPermissionErrorCount();

  long getUnknownTabletErrorCount();

  long getMutationArrayAvgSize();

  long getMutationArrayMinSize();

  long getMutationArrayMaxSize();

  long getCommitPrepCount();

  long getCommitPrepMinTime();

  long getCommitPrepMaxTime();

  long getCommitPrepAvgTime();

  long getConstraintViolationCount();

  long getWALogWriteCount();

  long getWALogWriteMinTime();

  long getWALogWriteMaxTime();

  long getWALogWriteAvgTime();

  long getCommitCount();

  long getCommitMinTime();

  long getCommitMaxTime();

  long getCommitAvgTime();

  void reset();
}
