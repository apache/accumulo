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
package org.apache.accumulo.core.util;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

public class MutableByteSequence extends ArrayByteSequence {
  private static final long serialVersionUID = 1L;

  public MutableByteSequence(byte[] data, int offset, int length) {
    super(data, offset, length);
  }

  public MutableByteSequence(ByteSequence bs) {
    super(new byte[Math.max(64, bs.length())]);
    System.arraycopy(bs.getBackingArray(), bs.offset(), data, 0, bs.length());
    this.length = bs.length();
    this.offset = 0;
  }

  public void setArray(byte[] data, int offset, int len) {
    this.data = data;
    this.offset = offset;
    this.length = len;
  }

  public void setLength(int len) {
    this.length = len;
  }
}
