/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.cleaner;

import static java.util.Objects.requireNonNull;

import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.clientImpl.TabletServerBatchWriter;
import org.apache.accumulo.core.singletons.SingletonReservation;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.slf4j.Logger;

/**
 * This class collects all the cleaner actions executed in various parts of the code.
 *
 * <p>
 * These actions replace the use of finalizers, which are deprecated in Java 9 and later, and should
 * be avoided. These actions are triggered by their respective objects when those objects become
 * phantom reachable.
 *
 * <p>
 * In the "unclosed*" methods below, the object should have been closed (implements AutoCloseable).
 * We could possibly consolidate these into a single method which only warns, and doesn't try to
 * clean up. We could also delete them entirely, since it is the caller's responsibility to close
 * AutoCloseable resources, not the object's own responsibility to detect that it wasn't closed.
 */
public class CleanerUtil {

  private static final Cleaner CLEANER = Cleaner.create();

  // helper method to keep logging consistent
  private static void logUnclosed(Logger log, String className, Exception e) {
    log.warn("{} found unreferenced without calling close()", className, e);
  }

  public static Cleanable unclosedMetaDataTableScanner(Object o, ScannerBase scanner,
      AtomicBoolean closed, Logger log) {
    requireNonNull(scanner);
    requireNonNull(closed);
    requireNonNull(log);
    // Exception is just for a stack trace in the log to instance left unclosed
    var e = new Exception();
    String className = o.getClass().getSimpleName();
    return CLEANER.register(o, () -> {
      if (!closed.get()) {
        logUnclosed(log, className, e);
        scanner.close();
      }
    });
  }

  public static Cleanable unclosedBatchScanner(BatchScanner o, ExecutorService es, Logger log) {
    requireNonNull(es);
    requireNonNull(log);
    // Exception is just for a stack trace in the log to instance left unclosed
    var e = new Exception();
    String className = BatchScanner.class.getSimpleName();
    return CLEANER.register(o, () -> {
      if (!es.isShutdown()) {
        logUnclosed(log, className, e);
        es.shutdownNow();
      }
    });
  }

  public static Cleanable unclosedMultiTableBatchWriter(MultiTableBatchWriter o,
      TabletServerBatchWriter tsbw, AtomicBoolean closed, Logger log) {
    requireNonNull(tsbw);
    requireNonNull(closed);
    requireNonNull(log);
    // Exception is just for a stack trace in the log to instance left unclosed
    var e = new Exception();
    String className = o.getClass().getSimpleName();
    return CLEANER.register(o, () -> {
      if (!closed.get()) {
        logUnclosed(log, className, e);
        try {
          tsbw.close();
        } catch (MutationsRejectedException mre) {
          log.error("{} internal error; exception closing {}", className,
              tsbw.getClass().getSimpleName(), mre);
        }
      }
    });
  }

  public static Cleanable unclosedClient(SingletonReservation o, AtomicBoolean closed, Logger log) {
    requireNonNull(closed);
    requireNonNull(log);
    // Exception is just for a stack trace in the log to instance left unclosed
    var e = new Exception();
    String className = AccumuloClient.class.getSimpleName();
    return CLEANER.register(o, () -> {
      if (!closed.get()) {
        logUnclosed(log, className, e);
      }
    });
  }

  // this done for the BatchWriterIterator test code; I don't trust that pattern, but
  // registering a cleaner is something any user is probably going to have to do to clean up
  // resources used in an iterator, until iterators properly implement their own close()
  public static Cleanable batchWriterAndClientCloser(Object o, BatchWriter bw,
      AccumuloClient client, Logger log) {
    requireNonNull(bw);
    requireNonNull(client);
    requireNonNull(log);
    return CLEANER.register(o, () -> {
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        log.error("Failed to close BatchWriter; some mutations may not be applied", e);
      } finally {
        client.close();
      }
    });
  }

  // this is dubious; MetadataConstraints should probably use the ZooCache provided by context
  // can be done in a follow-on action; this merely replaces the previous finalizer
  public static Cleanable zooCacheClearer(Object o, ZooCache zc) {
    requireNonNull(zc);
    return CLEANER.register(o, zc::clear);
  }

}
