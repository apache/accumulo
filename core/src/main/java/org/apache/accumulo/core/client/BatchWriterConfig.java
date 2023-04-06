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
package org.apache.accumulo.core.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_LATENCY_MAX;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_MEMORY_MAX;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_THREADS_MAX;
import static org.apache.accumulo.core.conf.ClientProperty.BATCH_WRITER_TIMEOUT_MAX;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * This object holds configuration settings used to instantiate a {@link BatchWriter}
 *
 * @since 1.5.0
 */
public class BatchWriterConfig implements Writable {

  private static final Long DEFAULT_MAX_MEMORY =
      ConfigurationTypeHelper.getMemoryAsBytes(BATCH_WRITER_MEMORY_MAX.getDefaultValue());
  private Long maxMemory = null;

  private static final Long DEFAULT_MAX_LATENCY =
      ConfigurationTypeHelper.getTimeInMillis(BATCH_WRITER_LATENCY_MAX.getDefaultValue());
  private Long maxLatency = null;

  private static final long DEFAULT_TIMEOUT = getDefaultTimeout();
  private Long timeout = null;

  private static final Integer DEFAULT_MAX_WRITE_THREADS =
      Integer.parseInt(BATCH_WRITER_THREADS_MAX.getDefaultValue());
  private Integer maxWriteThreads = null;

  private Durability durability = Durability.DEFAULT;
  private boolean isDurabilitySet = false;

  private static long getDefaultTimeout() {
    long defVal =
        ConfigurationTypeHelper.getTimeInMillis(BATCH_WRITER_TIMEOUT_MAX.getDefaultValue());
    if (defVal == 0L) {
      return Long.MAX_VALUE;
    } else {
      return defVal;
    }
  }

  /**
   * Sets the maximum memory to batch before writing. The smaller this value, the more frequently
   * the {@link BatchWriter} will write.<br>
   * If set to a value smaller than a single mutation, then it will {@link BatchWriter#flush()}
   * after each added mutation. Must be non-negative.
   *
   * <p>
   * <b>Default:</b> 50M
   *
   * @param maxMemory max size in bytes
   * @throws IllegalArgumentException if {@code maxMemory} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public BatchWriterConfig setMaxMemory(long maxMemory) {
    if (maxMemory < 0) {
      throw new IllegalArgumentException("Max memory must be non-negative.");
    }
    this.maxMemory = maxMemory;
    return this;
  }

  /**
   * Sets the maximum amount of time to hold the data in memory before flushing it to servers.<br>
   * For no maximum, set to zero, or {@link Long#MAX_VALUE} with {@link TimeUnit#MILLISECONDS}.
   *
   * <p>
   * {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be truncated to the nearest
   * {@link TimeUnit#MILLISECONDS}.<br>
   * If this truncation would result in making the value zero when it was specified as non-zero,
   * then a minimum value of one {@link TimeUnit#MILLISECONDS} will be used.
   *
   * <p>
   * <b>Default:</b> 120 seconds
   *
   * @param maxLatency the maximum latency, in the unit specified by the value of {@code timeUnit}
   * @param timeUnit determines how {@code maxLatency} will be interpreted
   * @throws IllegalArgumentException if {@code maxLatency} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public BatchWriterConfig setMaxLatency(long maxLatency, TimeUnit timeUnit) {
    if (maxLatency < 0) {
      throw new IllegalArgumentException("Negative max latency not allowed " + maxLatency);
    }

    if (maxLatency == 0) {
      this.maxLatency = Long.MAX_VALUE;
    } else {
      // make small, positive values that truncate to 0 when converted use the minimum millis
      // instead
      this.maxLatency = Math.max(1, timeUnit.toMillis(maxLatency));
    }
    return this;
  }

  /**
   * Sets the maximum amount of time an unresponsive server will be re-tried. When this timeout is
   * exceeded, the {@link BatchWriter} should throw an exception.<br>
   * For no timeout, set to zero, or {@link Long#MAX_VALUE} with {@link TimeUnit#MILLISECONDS}.
   *
   * <p>
   * {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be truncated to the nearest
   * {@link TimeUnit#MILLISECONDS}.<br>
   * If this truncation would result in making the value zero when it was specified as non-zero,
   * then a minimum value of one {@link TimeUnit#MILLISECONDS} will be used.
   *
   * <p>
   * <b>Default:</b> {@link Long#MAX_VALUE} (no timeout)
   *
   * @param timeout the timeout, in the unit specified by the value of {@code timeUnit}
   * @param timeUnit determines how {@code timeout} will be interpreted
   * @throws IllegalArgumentException if {@code timeout} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public BatchWriterConfig setTimeout(long timeout, TimeUnit timeUnit) {
    if (timeout < 0) {
      throw new IllegalArgumentException("Negative timeout not allowed " + timeout);
    }

    if (timeout == 0) {
      this.timeout = Long.MAX_VALUE;
    } else {
      // make small, positive values that truncate to 0 when converted use the minimum millis
      // instead
      this.timeout = Math.max(1, timeUnit.toMillis(timeout));
    }
    return this;
  }

  /**
   * Sets the maximum number of threads to use for writing data to the tablet servers.
   *
   * <p>
   * <b>Default:</b> 3
   *
   * @param maxWriteThreads the maximum threads to use
   * @throws IllegalArgumentException if {@code maxWriteThreads} is non-positive
   * @return {@code this} to allow chaining of set methods
   */
  public BatchWriterConfig setMaxWriteThreads(int maxWriteThreads) {
    if (maxWriteThreads <= 0) {
      throw new IllegalArgumentException("Max threads must be positive " + maxWriteThreads);
    }

    this.maxWriteThreads = maxWriteThreads;
    return this;
  }

  public long getMaxMemory() {
    return maxMemory != null ? maxMemory : DEFAULT_MAX_MEMORY;
  }

  public long getMaxLatency(TimeUnit timeUnit) {
    return timeUnit.convert(maxLatency != null ? maxLatency : DEFAULT_MAX_LATENCY, MILLISECONDS);
  }

  public long getTimeout(TimeUnit timeUnit) {
    return timeUnit.convert(timeout != null ? timeout : DEFAULT_TIMEOUT, MILLISECONDS);
  }

  public int getMaxWriteThreads() {
    return maxWriteThreads != null ? maxWriteThreads : DEFAULT_MAX_WRITE_THREADS;
  }

  /**
   * @since 1.7.0
   * @return the durability to be used by the BatchWriter
   */
  public Durability getDurability() {
    return durability;
  }

  /**
   * Change the durability for the BatchWriter session. The default durability is "default" which is
   * the table's durability setting. If the durability is set to something other than the default,
   * it will override the durability setting of the table.
   *
   * @param durability the Durability to be used by the BatchWriter
   * @since 1.7.0
   *
   */
  public BatchWriterConfig setDurability(Durability durability) {
    this.durability = durability;
    isDurabilitySet = true;
    return this;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write this out in a human-readable way
    ArrayList<String> fields = new ArrayList<>();
    if (maxMemory != null) {
      addField(fields, "maxMemory", maxMemory);
    }
    if (maxLatency != null) {
      addField(fields, "maxLatency", maxLatency);
    }
    if (maxWriteThreads != null) {
      addField(fields, "maxWriteThreads", maxWriteThreads);
    }
    if (timeout != null) {
      addField(fields, "timeout", timeout);
    }
    if (durability != Durability.DEFAULT) {
      addField(fields, "durability", durability);
    }
    String output = StringUtils.join(",", fields);

    byte[] bytes = output.getBytes(UTF_8);
    byte[] len = String.format("%6s#", Integer.toString(bytes.length, 36)).getBytes(UTF_8);
    if (len.length != 7) {
      throw new IllegalStateException("encoded length does not match expected value");
    }
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
    String strLen = new String(len, UTF_8);
    if (!strLen.endsWith("#")) {
      throw new IllegalStateException("length was not encoded correctly");
    }
    byte[] bytes = new byte[Integer
        .parseInt(strLen.substring(strLen.lastIndexOf(' ') + 1, strLen.length() - 1), 36)];
    in.readFully(bytes);

    String strFields = new String(bytes, UTF_8);
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
      } else if ("durability".equals(key)) {
        durability = DurabilityImpl.fromString(value);
      } else {
        /* ignore any other properties */
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BatchWriterConfig) {
      BatchWriterConfig other = (BatchWriterConfig) o;

      if (maxMemory != null) {
        if (!maxMemory.equals(other.maxMemory)) {
          return false;
        }
      } else {
        if (other.maxMemory != null) {
          return false;
        }
      }

      if (maxLatency != null) {
        if (!maxLatency.equals(other.maxLatency)) {
          return false;
        }
      } else {
        if (other.maxLatency != null) {
          return false;
        }
      }

      if (maxWriteThreads != null) {
        if (!maxWriteThreads.equals(other.maxWriteThreads)) {
          return false;
        }
      } else {
        if (other.maxWriteThreads != null) {
          return false;
        }
      }

      if (timeout != null) {
        if (!timeout.equals(other.timeout)) {
          return false;
        }
      } else {
        if (other.timeout != null) {
          return false;
        }
      }
      return durability == other.durability;
    }

    return false;
  }

  private static <T> T merge(T o1, T o2) {
    if (o1 != null) {
      return o1;
    }
    return o2;
  }

  /**
   * Merge this BatchWriterConfig with another. If config is set in both, preference will be given
   * to this config.
   *
   * @param other Another BatchWriterConfig
   * @return Merged BatchWriterConfig
   * @since 2.0.0
   */
  public BatchWriterConfig merge(BatchWriterConfig other) {
    BatchWriterConfig result = new BatchWriterConfig();
    result.maxMemory = merge(this.maxMemory, other.maxMemory);
    result.maxLatency = merge(this.maxLatency, other.maxLatency);
    result.timeout = merge(this.timeout, other.timeout);
    result.maxWriteThreads = merge(this.maxWriteThreads, other.maxWriteThreads);
    if (this.isDurabilitySet) {
      result.durability = this.durability;
    } else if (other.isDurabilitySet) {
      result.durability = other.durability;
    }
    return result;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(maxMemory).append(maxLatency).append(maxWriteThreads).append(timeout)
        .append(durability);
    return hcb.toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(32);
    sb.append("[maxMemory=").append(getMaxMemory()).append(", maxLatency=")
        .append(getMaxLatency(MILLISECONDS)).append(", maxWriteThreads=")
        .append(getMaxWriteThreads()).append(", timeout=").append(getTimeout(MILLISECONDS))
        .append(", durability=").append(durability).append("]");
    return sb.toString();
  }
}
