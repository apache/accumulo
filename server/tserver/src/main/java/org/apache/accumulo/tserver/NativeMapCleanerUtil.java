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

import java.lang.ref.Cleaner;
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

  // Chose 7 cleaners because each cleaner creates a thread, so do not want too many threads. This
  // should reduce lock contention for scans by a 7th vs a single cleaner, so that is good
  // reduction and 7 does not seem like too many threads to add. This array is indexed using
  // pointers addresses from native code, so there is a good chance those are memory aligned on
  // a multiple of 4, 8, 16, etc. So if changing the array size avoid multiples of 2.
  private static final Cleaner[] NMI_CLEANERS = new Cleaner[7];

  static {
    for (int i = 0; i < NMI_CLEANERS.length; i++) {
      NMI_CLEANERS[i] = Cleaner.create();
    }
  }

  public static Cleanable deleteNMIterator(Object obj, AtomicLong nmiPtr) {
    requireNonNull(nmiPtr);

    long ptr = Math.abs(nmiPtr.get());
    if (ptr == Long.MIN_VALUE) {
      ptr = 0;
    }
    int cleanerIndex = (int) (ptr % NMI_CLEANERS.length);

    // This method can be called very frequently by many scan threads. The register call on cleaner
    // acquires a lock which can cause lock contention between threads. Having multiple cleaners for
    // this case lowers the amount of lock contention among scan threads. This locking was observed
    // in jdk.internal.rf.PhantomCleanable.insert() which currently has a synchronized code block.
    return NMI_CLEANERS[cleanerIndex].register(obj, () -> {
      long nmiPointer = nmiPtr.get();
      if (nmiPointer != 0) {
        NativeMap._deleteNMI(nmiPointer);
      }
    });
  }
}
