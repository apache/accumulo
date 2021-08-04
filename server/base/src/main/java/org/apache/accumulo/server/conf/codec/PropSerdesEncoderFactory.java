/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.codec;

/**
 * Create an serialization / deserialization encoder based on serialization version and additional
 * information used in the serialization process.
 */
public class PropSerdesEncoderFactory {

  private final PropSerdes serdes;

  public PropSerdesEncoderFactory(final EncodingOptions encodingOptions) {

    // currently only one version supported - this would need to be extended to support others.
    if (encodingOptions.getEncodingVersion().equals(EncodingOptions.EncodingVersion.V1_0)) {
      serdes = new GzipPropEncoding(encodingOptions);
    } else {
      throw new IllegalArgumentException("Unsupported encoding options " + encodingOptions);
    }

  }

  /**
   * Get the serialization / deserialization encoder created.
   *
   * @return a serialization / deserialization encoder.
   */
  public PropSerdes getSerdes() {
    return serdes;
  }

}
