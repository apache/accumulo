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
package org.apache.accumulo.core.data;

import java.util.Objects;

/**
 * An immutable implementation of {@link ByteSequence} specialized for the empty case.
 *
 * <p>
 * This class is package private because direct construction is not expected, instead use of
 * {@link ByteSequence#of()} is how instances of this should be created.
 * </p>
 */
final class EmptyByteSequence extends ByteSequence {

  private static final long serialVersionUID = 1L;

  static final ByteSequence INSTANCE = new EmptyByteSequence();

  private static final byte[] EMPTY = new byte[0];

  // constructor is private because this class is a singleton
  private EmptyByteSequence() {}

  @Override
  public byte byteAt(int i) {
    throw new IndexOutOfBoundsException();
  }

  @Override
  public int length() {
    return 0;
  }

  @Override
  public ByteSequence subSequence(int start, int end) {
    Objects.checkFromToIndex(start, end, 0);
    return this;
  }

  @Override
  public byte[] toArray() {
    return EMPTY;
  }

  @Override
  public boolean isBackedByArray() {
    return true;
  }

  @Override
  public byte[] getBackingArray() {
    return EMPTY;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int hashCode() {
    return 1;
  }

  @Override
  public String toString() {
    return "";
  }
}
