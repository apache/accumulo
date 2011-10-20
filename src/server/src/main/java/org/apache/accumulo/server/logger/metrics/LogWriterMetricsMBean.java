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
package org.apache.accumulo.server.logger.metrics;

public interface LogWriterMetricsMBean {
  
  public static final String close = "close";
  public static final String copy = "copy";
  public static final String create = "create";
  public static final String logAppend = "logAppend";
  public static final String logFlush = "logFlush";
  public static final String logException = "logException";
  
  public long getCloseCount();
  
  public long getCloseAvgTime();
  
  public long getCloseMinTime();
  
  public long getCloseMaxTime();
  
  public long getCopyCount();
  
  public long getCopyAvgTime();
  
  public long getCopyMinTime();
  
  public long getCopyMaxTime();
  
  public long getCreateCount();
  
  public long getCreateMinTime();
  
  public long getCreateMaxTime();
  
  public long getCreateAvgTime();
  
  public long getLogAppendCount();
  
  public long getLogAppendMinTime();
  
  public long getLogAppendMaxTime();
  
  public long getLogAppendAvgTime();
  
  public long getLogFlushCount();
  
  public long getLogFlushMinTime();
  
  public long getLogFlushMaxTime();
  
  public long getLogFlushAvgTime();
  
  public long getLogExceptionCount();
  
  public void reset();
  
}
