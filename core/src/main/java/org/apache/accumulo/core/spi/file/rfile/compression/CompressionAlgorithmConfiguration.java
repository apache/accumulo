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
package org.apache.accumulo.core.spi.file.rfile.compression;

public interface CompressionAlgorithmConfiguration {

  /**
   * @return algorithm alias
   */
  String getName();

  /**
   * @return name of property that can be specified in configuration or in system properties to
   *         override class name of codec
   */
  String getCodecClassName();

  /**
   * @return fully qualified class name of codec
   */
  String getCodecClassNameProperty();

  /**
   * @return default buffer size for compression algorithm
   */
  int getDefaultBufferSize();

  /**
   * @return name of property that can be specified in configuration or in system properties to
   *         override default buffer size
   */
  String getBufferSizeProperty();

  /**
   * @return true if codecs with non-default buffer sizes should be cached
   */
  default boolean cacheCodecsWithNonDefaultSizes() {
    return false;
  }

}
