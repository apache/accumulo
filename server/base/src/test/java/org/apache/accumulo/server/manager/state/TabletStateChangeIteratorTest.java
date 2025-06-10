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

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.DataOutputBuffer;

public class TabletStateChangeIteratorTest {

  // This is the algorithm used for encoding prior to 2.1.4. Duplicated here to test compatibility.
  private String oldEncode(Collection<KeyExtent> migrations) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (KeyExtent extent : migrations) {
        extent.writeTo(buffer);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));
  }

  // TODO test compat for encoding of old data?
}
