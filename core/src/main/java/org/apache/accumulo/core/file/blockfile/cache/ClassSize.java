/*
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.file.blockfile.cache;

import java.util.Properties;

import org.apache.accumulo.core.client.impl.ServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for determining the "size" of a class, an attempt to calculate the actual bytes that an object of this class will occupy in memory
 *
 * The core of this class is taken from the Derby project
 */
public class ClassSize {
  private static final Logger log = LoggerFactory.getLogger(ClassSize.class);

  /** Array overhead */
  public static final int ARRAY;

  /** Overhead for ArrayList(0) */
  public static final int ARRAYLIST;

  /** Overhead for ByteBuffer */
  public static final int BYTE_BUFFER;

  /** Overhead for an Integer */
  public static final int INTEGER;

  /** Overhead for entry in map */
  public static final int MAP_ENTRY;

  /** Object overhead is minimum 2 * reference size (8 bytes on 64-bit) */
  public static final int OBJECT;

  /** Reference size is 8 bytes on 64-bit, 4 bytes on 32-bit */
  public static final int REFERENCE;

  /** String overhead */
  public static final int STRING;

  /** Overhead for TreeMap */
  public static final int TREEMAP;

  /** Overhead for ConcurrentHashMap */
  public static final int CONCURRENT_HASHMAP;

  /** Overhead for ConcurrentHashMap.Entry */
  public static final int CONCURRENT_HASHMAP_ENTRY;

  /** Overhead for ConcurrentHashMap.Segment */
  public static final int CONCURRENT_HASHMAP_SEGMENT;

  /** Overhead for ConcurrentSkipListMap */
  public static final int CONCURRENT_SKIPLISTMAP;

  /** Overhead for ConcurrentSkipListMap Entry */
  public static final int CONCURRENT_SKIPLISTMAP_ENTRY;

  /** Overhead for ReentrantReadWriteLock */
  public static final int REENTRANT_LOCK;

  /** Overhead for AtomicLong */
  public static final int ATOMIC_LONG;

  /** Overhead for AtomicInteger */
  public static final int ATOMIC_INTEGER;

  /** Overhead for AtomicBoolean */
  public static final int ATOMIC_BOOLEAN;

  /** Overhead for CopyOnWriteArraySet */
  public static final int COPYONWRITE_ARRAYSET;

  /** Overhead for CopyOnWriteArrayList */
  public static final int COPYONWRITE_ARRAYLIST;

  private static final String THIRTY_TWO = "32";

  /**
   * Method for reading the arc settings and setting overheads according to 32-bit or 64-bit architecture.
   */
  static {
    // Figure out whether this is a 32 or 64 bit machine.
    Properties sysProps = System.getProperties();
    String arcModel = sysProps.getProperty("sun.arch.data.model");

    // Default value is set to 8, covering the case when arcModel is unknown
    REFERENCE = arcModel.equals(THIRTY_TWO) ? 4 : 8;

    OBJECT = 2 * REFERENCE;

    ARRAY = 3 * REFERENCE;

    ARRAYLIST = align(OBJECT + align(REFERENCE) + align(ARRAY) + (2 * SizeConstants.SIZEOF_INT));

    BYTE_BUFFER = align(OBJECT + align(REFERENCE) + align(ARRAY) + (5 * SizeConstants.SIZEOF_INT) + (3 * SizeConstants.SIZEOF_BOOLEAN)
        + SizeConstants.SIZEOF_LONG);

    INTEGER = align(OBJECT + SizeConstants.SIZEOF_INT);

    MAP_ENTRY = align(OBJECT + 5 * REFERENCE + SizeConstants.SIZEOF_BOOLEAN);

    TREEMAP = align(OBJECT + (2 * SizeConstants.SIZEOF_INT) + align(7 * REFERENCE));

    STRING = align(OBJECT + ARRAY + REFERENCE + 3 * SizeConstants.SIZEOF_INT);

    CONCURRENT_HASHMAP = align((2 * SizeConstants.SIZEOF_INT) + ARRAY + (6 * REFERENCE) + OBJECT);

    CONCURRENT_HASHMAP_ENTRY = align(REFERENCE + OBJECT + (3 * REFERENCE) + (2 * SizeConstants.SIZEOF_INT));

    CONCURRENT_HASHMAP_SEGMENT = align(REFERENCE + OBJECT + (3 * SizeConstants.SIZEOF_INT) + SizeConstants.SIZEOF_FLOAT + ARRAY);

    CONCURRENT_SKIPLISTMAP = align(SizeConstants.SIZEOF_INT + OBJECT + (8 * REFERENCE));

    CONCURRENT_SKIPLISTMAP_ENTRY = align(align(OBJECT + (3 * REFERENCE)) + /* one node per entry */
    align((OBJECT + (3 * REFERENCE)) / 2)); /* one index per two entries */

    REENTRANT_LOCK = align(OBJECT + (3 * REFERENCE));

    ATOMIC_LONG = align(OBJECT + SizeConstants.SIZEOF_LONG);

    ATOMIC_INTEGER = align(OBJECT + SizeConstants.SIZEOF_INT);

    ATOMIC_BOOLEAN = align(OBJECT + SizeConstants.SIZEOF_BOOLEAN);

    COPYONWRITE_ARRAYSET = align(OBJECT + REFERENCE);

    COPYONWRITE_ARRAYLIST = align(OBJECT + (2 * REFERENCE) + ARRAY);
  }

  /**
<<<<<<< HEAD
=======
   * The estimate of the size of a class instance depends on whether the JVM uses 32 or 64 bit addresses, that is it depends on the size of an object reference.
   * It is a linear function of the size of a reference, e.g. 24 + 5*r where r is the size of a reference (usually 4 or 8 bytes).
   *
   * This method returns the coefficients of the linear function, e.g. {24, 5} in the above example.
   *
   * @param cl
   *          A class whose instance size is to be estimated
   * @return an array of 3 integers. The first integer is the size of the primitives, the second the number of arrays and the third the number of references.
   */
  private static int[] getSizeCoefficients(Class<?> cl, boolean debug) {
    int primitives = 0;
    int arrays = 0;
    // The number of references that a new object takes
    int references = nrOfRefsPerObj;

    for (; null != cl; cl = cl.getSuperclass()) {
      Field[] field = cl.getDeclaredFields();
      if (null != field) {
        for (int i = 0; i < field.length; i++) {
          if (!Modifier.isStatic(field[i].getModifiers())) {
            Class<?> fieldClass = field[i].getType();
            if (fieldClass.isArray()) {
              arrays++;
              references++;
            } else if (!fieldClass.isPrimitive()) {
              references++;
            } else {// Is simple primitive
              String name = fieldClass.getName();

              if (name.equals("int") || name.equals("I"))
                primitives += SizeConstants.SIZEOF_INT;
              else if (name.equals("long") || name.equals("J"))
                primitives += SizeConstants.SIZEOF_LONG;
              else if (name.equals("boolean") || name.equals("Z"))
                primitives += SizeConstants.SIZEOF_BOOLEAN;
              else if (name.equals("short") || name.equals("S"))
                primitives += SizeConstants.SIZEOF_SHORT;
              else if (name.equals("byte") || name.equals("B"))
                primitives += SizeConstants.SIZEOF_BYTE;
              else if (name.equals("char") || name.equals("C"))
                primitives += SizeConstants.SIZEOF_CHAR;
              else if (name.equals("float") || name.equals("F"))
                primitives += SizeConstants.SIZEOF_FLOAT;
              else if (name.equals("double") || name.equals("D"))
                primitives += SizeConstants.SIZEOF_DOUBLE;
            }
            if (debug) {
              if (log.isDebugEnabled()) {
                // Write out region name as string and its encoded name.
                log.debug("{}\n\t{}", field[i].getName(), field[i].getType());
              }
            }
          }
        }
      }
    }
    return new int[] {primitives, arrays, references};
  }

  /**
   * Estimate the static space taken up by a class instance given the coefficients returned by getSizeCoefficients.
   *
   * @param coeff
   *          the coefficients
   *
   * @return the size estimate, in bytes
   */
  private static long estimateBaseFromCoefficients(int[] coeff, boolean debug) {
    long size = coeff[0] + align(coeff[1] * ARRAY) + coeff[2] * REFERENCE;

    // Round up to a multiple of 8
    size = align(size);
    if (debug) {
      if (log.isDebugEnabled()) {
        // Write out region name as string and its encoded name.
        log.debug("Primitives {}, arrays {}, references(includes {} for object overhead) {}, refSize {}, size {}", coeff[0], coeff[1], nrOfRefsPerObj, coeff[2], REFERENCE, size);
      }
    }
    return size;
  }

  /**
   * Estimate the static space taken up by the fields of a class. This includes the space taken up by by references (the pointer) but not by the referenced
   * object. So the estimated size of an array field does not depend on the size of the array. Similarly the size of an object (reference) field does not depend
   * on the object.
   *
   * @return the size estimate in bytes.
   */
  public static long estimateBase(Class<?> cl, boolean debug) {
    return estimateBaseFromCoefficients(getSizeCoefficients(cl, debug), debug);
  }

  /**
>>>>>>> ACCUMULO-3652 refactor for slf4j all packages except gc, master, monitor and server-base
   * Aligns a number to 8.
   *
   * @param num
   *          number to align to 8
   * @return smallest number >= input that is a multiple of 8
   */
  public static int align(int num) {
    return (int) (align((long) num));
  }

  /**
   * Aligns a number to 8.
   *
   * @param num
   *          number to align to 8
   * @return smallest number >= input that is a multiple of 8
   */
  public static long align(long num) {
    // The 7 comes from that the alignSize is 8 which is the number of bytes
    // stored and sent together
    return ((num + 7) >> 3) << 3;
  }

}
