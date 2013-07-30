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
  
  public final static String permissionErrors = "permissionErrors";
  public final static String unknownTabletErrors = "unknownTabletErrors";
  public final static String mutationArraySize = "mutationArraysSize";
  public final static String commitPrep = "commitPrep";
  public final static String constraintViolations = "constraintViolations";
  public final static String waLogWriteTime = "waLogWriteTime";
  public final static String commitTime = "commitTime";
  
  public long getPermissionErrorCount();
  
  public long getUnknownTabletErrorCount();
  
  public long getMutationArrayAvgSize();
  
  public long getMutationArrayMinSize();
  
  public long getMutationArrayMaxSize();
  
  public long getCommitPrepCount();
  
  public long getCommitPrepMinTime();
  
  public long getCommitPrepMaxTime();
  
  public long getCommitPrepAvgTime();
  
  public long getConstraintViolationCount();
  
  public long getWALogWriteCount();
  
  public long getWALogWriteMinTime();
  
  public long getWALogWriteMaxTime();
  
  public long getWALogWriteAvgTime();
  
  public long getCommitCount();
  
  public long getCommitMinTime();
  
  public long getCommitMaxTime();
  
  public long getCommitAvgTime();
  
  public void reset();
}
