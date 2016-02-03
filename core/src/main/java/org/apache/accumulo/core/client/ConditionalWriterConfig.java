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
package org.apache.accumulo.core.client;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.security.Authorizations;

/**
 *
 * @since 1.6.0
 */
public class ConditionalWriterConfig {

  private static final Long DEFAULT_TIMEOUT = Long.MAX_VALUE;
  private Long timeout = null;

  private static final Integer DEFAULT_MAX_WRITE_THREADS = 3;
  private Integer maxWriteThreads = null;

  private Authorizations auths = Authorizations.EMPTY;

  private Durability durability = Durability.DEFAULT;

  private String classLoaderContext = null;

  /**
   * A set of authorization labels that will be checked against the column visibility of each key in order to filter data. The authorizations passed in must be
   * a subset of the accumulo user's set of authorizations. If the accumulo user has authorizations (A1, A2) and authorizations (A2, A3) are passed, then an
   * exception will be thrown.
   *
   * <p>
   * Any condition that is not visible with this set of authorizations will fail.
   */
  public ConditionalWriterConfig setAuthorizations(Authorizations auths) {
    checkArgument(auths != null, "auths is null");
    this.auths = auths;
    return this;
  }

  /**
   * Sets the maximum amount of time an unresponsive server will be re-tried. When this timeout is exceeded, the {@link ConditionalWriter} should return the
   * mutation with an exception.<br>
   * For no timeout, set to zero, or {@link Long#MAX_VALUE} with {@link TimeUnit#MILLISECONDS}.
   *
   * <p>
   * {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be truncated to the nearest {@link TimeUnit#MILLISECONDS}.<br>
   * If this truncation would result in making the value zero when it was specified as non-zero, then a minimum value of one {@link TimeUnit#MILLISECONDS} will
   * be used.
   *
   * <p>
   * <b>Default:</b> {@link Long#MAX_VALUE} (no timeout)
   *
   * @param timeout
   *          the timeout, in the unit specified by the value of {@code timeUnit}
   * @param timeUnit
   *          determines how {@code timeout} will be interpreted
   * @throws IllegalArgumentException
   *           if {@code timeout} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public ConditionalWriterConfig setTimeout(long timeout, TimeUnit timeUnit) {
    if (timeout < 0)
      throw new IllegalArgumentException("Negative timeout not allowed " + timeout);

    if (timeout == 0)
      this.timeout = Long.MAX_VALUE;
    else
      // make small, positive values that truncate to 0 when converted use the minimum millis instead
      this.timeout = Math.max(1, timeUnit.toMillis(timeout));
    return this;
  }

  /**
   * Sets the maximum number of threads to use for writing data to the tablet servers.
   *
   * <p>
   * <b>Default:</b> 3
   *
   * @param maxWriteThreads
   *          the maximum threads to use
   * @throws IllegalArgumentException
   *           if {@code maxWriteThreads} is non-positive
   * @return {@code this} to allow chaining of set methods
   */
  public ConditionalWriterConfig setMaxWriteThreads(int maxWriteThreads) {
    if (maxWriteThreads <= 0)
      throw new IllegalArgumentException("Max threads must be positive " + maxWriteThreads);

    this.maxWriteThreads = maxWriteThreads;
    return this;
  }

  /**
   * Sets the Durability for the mutation, if applied.
   * <p>
   * <b>Default:</b> Durability.DEFAULT: use the table's durability configuration.
   *
   * @return {@code this} to allow chaining of set methods
   * @since 1.7.0
   */
  public ConditionalWriterConfig setDurability(Durability durability) {
    this.durability = durability;
    return this;
  }

  public Authorizations getAuthorizations() {
    return auths;
  }

  public long getTimeout(TimeUnit timeUnit) {
    return timeUnit.convert(timeout != null ? timeout : DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  public int getMaxWriteThreads() {
    return maxWriteThreads != null ? maxWriteThreads : DEFAULT_MAX_WRITE_THREADS;
  }

  public Durability getDurability() {
    return durability;
  }

  /**
   * Sets the name of the classloader context on this scanner. See the administration chapter of the user manual for details on how to configure and use
   * classloader contexts.
   *
   * @param classLoaderContext
   *          name of the classloader context
   * @throws NullPointerException
   *           if context is null
   * @since 1.8.0
   */
  public void setClassLoaderContext(String classLoaderContext) {
    requireNonNull(classLoaderContext, "context name cannot be null");
    this.classLoaderContext = classLoaderContext;
  }

  /**
   * Clears the current classloader context set on this scanner
   *
   * @since 1.8.0
   */
  public void clearClassLoaderContext() {
    this.classLoaderContext = null;
  }

  /**
   * Returns the name of the current classloader context set on this scanner
   *
   * @return name of the current context
   * @since 1.8.0
   */
  public String getClassLoaderContext() {
    return this.classLoaderContext;
  }

}
