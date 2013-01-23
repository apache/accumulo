/**
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
package org.apache.accumulo.core.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * This object holds configuration settings used to instantiate a {@link BatchWriter}
 */
public class BatchWriterConfig implements Writable {
  
  private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
  private Long maxMemory = null;
  
  private static final Long DEFAULT_MAX_LATENCY = 2 * 60 * 1000l;
  private Long maxLatency = null;
  
  private static final Long DEFAULT_TIMEOUT = Long.MAX_VALUE;
  private Long timeout = null;
  
  private static final Integer DEFAULT_MAX_WRITE_THREADS = 3;
  private Integer maxWriteThreads = null;
  
  /**
   * 
   * @param maxMemory
   *          size in bytes of the maximum memory to batch before writing. Minimum 1K. Defaults to 50M.
   */
  
  public BatchWriterConfig setMaxMemory(long maxMemory) {
    if (maxMemory < 1024)
      throw new IllegalArgumentException("Max memory is too low at " + maxMemory + ". Minimum 1K.");
    this.maxMemory = maxMemory;
    return this;
  }
  
  /**
   * @param maxLatency
   *          The maximum amount of time to hold data in memory before flushing it to servers. For no max set to zero or Long.MAX_VALUE with TimeUnit.MILLIS.
   *          Defaults to 120 seconds.
   * @param timeUnit
   *          Determines how maxLatency will be interpreted.
   * @return this to allow chaining of set methods
   */
  
  public BatchWriterConfig setMaxLatency(long maxLatency, TimeUnit timeUnit) {
    if (maxLatency < 0)
      throw new IllegalArgumentException("Negative max latency not allowed " + maxLatency);
    
    if (maxLatency == 0)
      this.maxLatency = Long.MAX_VALUE;
    else
      this.maxLatency = timeUnit.toMillis(maxLatency);
    return this;
  }
  
  /**
   * 
   * @param timeout
   *          The maximum amount of time an unresponsive server will be retried. When this timeout is exceeded, the BatchWriter should throw an exception. For
   *          no timeout set to zero or Long.MAX_VALUE with TimeUnit.MILLIS. Defaults to no timeout.
   * @param timeUnit
   * @return this to allow chaining of set methods
   */
  
  public BatchWriterConfig setTimeout(long timeout, TimeUnit timeUnit) {
    if (timeout < 0)
      throw new IllegalArgumentException("Negative timeout not allowed " + timeout);
    
    if (timeout == 0)
      timeout = Long.MAX_VALUE;
    else
      this.timeout = timeUnit.toMillis(timeout);
    return this;
  }
  
  /**
   * @param maxWriteThreads
   *          the maximum number of threads to use for writing data to the tablet servers. Defaults to 3.
   * @return this to allow chaining of set methods
   */
  
  public BatchWriterConfig setMaxWriteThreads(int maxWriteThreads) {
    if (maxWriteThreads <= 0)
      throw new IllegalArgumentException("Max threads must be positive " + maxWriteThreads);
    
    this.maxWriteThreads = maxWriteThreads;
    return this;
  }
  
  public long getMaxMemory() {
    return maxMemory != null ? maxMemory : DEFAULT_MAX_MEMORY;
  }
  
  public long getMaxLatency(TimeUnit timeUnit) {
    return timeUnit.convert(maxLatency != null ? maxLatency : DEFAULT_MAX_LATENCY, TimeUnit.MILLISECONDS);
  }
  
  public long getTimeout(TimeUnit timeUnit) {
    return timeUnit.convert(timeout != null ? timeout : DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
  }
  
  public int getMaxWriteThreads() {
    return maxWriteThreads != null ? maxWriteThreads : DEFAULT_MAX_WRITE_THREADS;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    // write this out in a human-readable way
    ArrayList<String> fields = new ArrayList<String>();
    if (maxMemory != null)
      addField(fields, "maxMemory", maxMemory);
    if (maxLatency != null)
      addField(fields, "maxLatency", maxLatency);
    if (maxWriteThreads != null)
      addField(fields, "maxWriteThreads", maxWriteThreads);
    if (timeout != null)
      addField(fields, "timeout", timeout);
    String output = StringUtils.join(",", fields);
    
    byte[] bytes = output.getBytes(Charset.forName("UTF-8"));
    byte[] len = String.format("%6s#", Integer.toString(bytes.length, 36)).getBytes("UTF-8");
    if (len.length != 7)
      throw new IllegalStateException("encoded length does not match expected value");
    out.write(len);
    out.write(bytes);
  }
  
  private void addField(List<String> fields, String name, Object value) {
    String key = StringUtils.escapeString(name, '\\', new char[] {',', '='});
    String val = StringUtils.escapeString(String.valueOf(value), '\\', new char[] {',', '='});
    fields.add(key + '=' + val);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] len = new byte[7];
    in.readFully(len);
    String strLen = new String(len, Charset.forName("UTF-8"));
    if (!strLen.endsWith("#"))
      throw new IllegalStateException("length was not encoded correctly");
    byte[] bytes = new byte[Integer.parseInt(strLen.substring(strLen.lastIndexOf(' ') + 1, strLen.length() - 1), 36)];
    in.readFully(bytes);
    
    String strFields = new String(bytes, Charset.forName("UTF-8"));
    String[] fields = StringUtils.split(strFields, '\\', ',');
    for (String field : fields) {
      String[] keyValue = StringUtils.split(field, '\\', '=');
      String key = keyValue[0];
      String value = keyValue[1];
      if ("maxMemory".equals(key)) {
        maxMemory = Long.valueOf(value);
      } else if ("maxLatency".equals(key)) {
        maxLatency = Long.valueOf(value);
      } else if ("maxWriteThreads".equals(key)) {
        maxWriteThreads = Integer.valueOf(value);
      } else if ("timeout".equals(key)) {
        timeout = Long.valueOf(value);
      } else {
        /* ignore any other properties */
      }
    }
  }
}
