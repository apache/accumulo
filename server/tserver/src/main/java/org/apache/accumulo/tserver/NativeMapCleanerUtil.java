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
package org.apache.accumulo.tserver;

import static java.util.Objects.requireNonNull;

import java.lang.ref.Cleaner.Cleanable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.util.cleaner.CleanerUtil;
import org.slf4j.Logger;

/**
 * A cleaner utility for NativeMap code, in the same spirit as {@link CleanerUtil}.
 */
public class NativeMapCleanerUtil {

  public static Cleanable deleteNM(Object obj, Logger log, AtomicLong nmPtr) {
    requireNonNull(nmPtr);
    requireNonNull(log);
    return CleanerUtil.CLEANER.register(obj, () -> {
      long nmPointer = nmPtr.get();
      if (nmPointer != 0) {
        log.warn(String.format("Deallocating native map 0x%016x in finalize", nmPointer));
        NativeMap._deleteNativeMap(nmPointer);
      }
    });
  }

  public static Cleanable deleteNMIterator(Object obj, AtomicLong nmiPtr) {
    requireNonNull(nmiPtr);
    return CleanerUtil.CLEANER.register(obj, () -> {
      long nmiPointer = nmiPtr.get();
      if (nmiPointer != 0) {
        NativeMap._deleteNMI(nmiPointer);
      }
    });
  }

}
