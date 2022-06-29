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
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.fate.FateTxIdUtil;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * A strongly typed representation of a Fate Transaction ID. There are two representations of a
 * FateTxId. The first is a hexadecimal long. The second is a formatted String.
 *
 * @since 2.1.0
 */
public class FateTxId extends AbstractNumericId<FateTxId> {
  private static final long serialVersionUID = 1L;

  // cache is for canonicalization/deduplication of created objects,
  // to limit the number of TableId objects in the JVM at any given moment
  // WeakReferences are used because we don't need them to stick around any longer than they need to
  static final Cache<Long,FateTxId> cache = CacheBuilder.newBuilder().weakValues().build();

  private FateTxId(long hexadecimal) {
    super(hexadecimal);
  }

  /**
   * Get a FateTxId object for the provided hexadecimal long. This is guaranteed to be non-null.
   *
   * @param hexadecimal
   *          Fate transaction ID as a hexadecimal long
   * @return FateTxId object
   */
  public static FateTxId of(long hexadecimal) {
    try {
      return cache.get(hexadecimal, () -> new FateTxId(hexadecimal));
    } catch (ExecutionException e) {
      throw new AssertionError(
          "This should never happen: ID constructor should never return null.");
    }
  }

  /**
   * Get a FateTxId object for the provided formatted String. This is guaranteed to be non-null.
   *
   * @param formattedString
   *          Fate transaction ID as a formatted String
   * @return FateTxId object
   * @throws IllegalArgumentException
   *           if formattedString is not formatted properly.
   */
  public static FateTxId of(String formattedString) {
    Objects.requireNonNull(formattedString);
    if (!FateTxIdUtil.isFormatedTid(formattedString)) {
      throw new IllegalArgumentException(
          "The string provided (" + formattedString + ") is not formatted properly. ");
    }
    return new FateTxId(FateTxIdUtil.fromString(formattedString));
  }

  /**
   * Get a FateTxId object by parsing a string. This method is useful when it is unknown whether the
   * string is a specially formatted TX string or a string of a hexadecimal long. This is guaranteed
   * to be non-null.
   *
   * @return FateTxId object
   */
  public static FateTxId parse(String txString) {
    Objects.requireNonNull(txString);
    return new FateTxId(FateTxIdUtil.parseHexLongFromString(txString));
  }

  @Override
  public String toString() {
    return "FATE[" + canonical() + "]";
  }

  public String toPlainString() {
    return "" + canonical();
  }

  public String toFormattedString() {
    return FateTxIdUtil.formatTid(canonical());
  }

}
