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
package org.apache.accumulo.core.file.blockfile.cache.impl;

/**
 * Class for determining the "size" of a class, an attempt to calculate the actual bytes that an
 * object of this class will occupy in memory
 *
 * The core of this class is taken from the Derby project
 */
public class ClassSize {

  /** Array overhead */
  public static final int ARRAY;

  /** Overhead for ByteBuffer */
  public static final int BYTE_BUFFER;

  /** Object overhead is minimum 2 * reference size (8 bytes on 64-bit) */
  public static final int OBJECT;

  /** Reference size is 8 bytes on 64-bit, 4 bytes on 32-bit */
  public static final int REFERENCE;

  /** String overhead */
  public static final int STRING;

  /** Overhead for ConcurrentHashMap */
  public static final int CONCURRENT_HASHMAP;

  /** Overhead for ConcurrentHashMap.Entry */
  public static final int CONCURRENT_HASHMAP_ENTRY;

  /** Overhead for ConcurrentHashMap.Segment */
  public static final int CONCURRENT_HASHMAP_SEGMENT;

  /** Overhead for AtomicInteger */
  public static final int ATOMIC_INTEGER;

  private static final String THIRTY_TWO = "32";

  /**
   * Method for reading the arc settings and setting overheads according to 32-bit or 64-bit
   * architecture.
   */
  static {
    // Figure out whether this is a 32 or 64 bit machine.
    String arcModel = System.getProperty("sun.arch.data.model");

    // Default value is set to 8, covering the case when arcModel is unknown
    REFERENCE = arcModel.equals(THIRTY_TWO) ? 4 : 8;

    OBJECT = 2 * REFERENCE;

    ARRAY = 3 * REFERENCE;

    BYTE_BUFFER = align(OBJECT + align(REFERENCE) + align(ARRAY) + (5 * SizeConstants.SIZEOF_INT)
        + (3 * SizeConstants.SIZEOF_BOOLEAN) + SizeConstants.SIZEOF_LONG);

    STRING = align(OBJECT + ARRAY + REFERENCE + 3 * SizeConstants.SIZEOF_INT);

    CONCURRENT_HASHMAP = align((2 * SizeConstants.SIZEOF_INT) + ARRAY + (6 * REFERENCE) + OBJECT);

    CONCURRENT_HASHMAP_ENTRY =
        align(REFERENCE + OBJECT + (3 * REFERENCE) + (2 * SizeConstants.SIZEOF_INT));

    CONCURRENT_HASHMAP_SEGMENT = align(
        REFERENCE + OBJECT + (3 * SizeConstants.SIZEOF_INT) + SizeConstants.SIZEOF_FLOAT + ARRAY);

    ATOMIC_INTEGER = align(OBJECT + SizeConstants.SIZEOF_INT);
  }

  /**
   * Aligns a number to 8.
   *
   * @param num number to align to 8
   * @return smallest number &gt;= input that is a multiple of 8
   */
  public static int align(int num) {
    return (int) align((long) num);
  }

  /**
   * Aligns a number to 8.
   *
   * @param num number to align to 8
   * @return smallest number &gt;= input that is a multiple of 8
   */
  public static long align(long num) {
    // The 7 comes from that the alignSize is 8 which is the number of bytes
    // stored and sent together
    return ((num + 7) >> 3) << 3;
  }

}
