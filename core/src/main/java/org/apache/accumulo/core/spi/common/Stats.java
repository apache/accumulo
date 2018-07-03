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
package org.apache.accumulo.core.spi.common;

/**
 * @since 2.0.0
 */
public interface Stats {

  /**
   * @return the minimum data point seen, or 0 if no data was seen
   */
  long min();

  /**
   * @return the maximum data point seen, or 0 if no data was seen
   */
  long max();

  /**
   * @return the mean of the data points seen, or {@link Double#NaN} if no data was seen
   */
  double mean();

  /**
   * @return the sum of the data points seen, or 0 if no data was seen
   */
  long sum();

  /**
   * @return the number of data points seen
   */
  long num();
}
