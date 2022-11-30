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
package org.apache.accumulo.core.util.cleaner;

import static java.util.Objects.requireNonNull;

import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
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

  public static final Cleaner CLEANER = Cleaner.create();

  /**
   * Register an action to warn about caller failing to close an {@link AutoCloseable} object.
   *
   * <p>
   * This task will register a generic action to:
   * <ol>
   * <li>check that the monitored object wasn't closed,
   * <li>log a warning that the monitored object was not closed,
   * <li>attempt to close a resource within the object, and
   * <li>log an error if the resource cannot be closed for any reason
   * </ol>
   *
   * @param obj the object to monitor for becoming phantom-reachable without having been closed
   * @param objClass the class whose simple name will be used in the log message for <code>o</code>
   *        (usually an interface name, rather than the actual impl name of the object)
   * @param closed a flag to check whether <code>o</code> has already been closed
   * @param log the logger to use when emitting error/warn messages
   * @param closeable the resource within <code>o</code> to close when <code>o</code> is cleaned;
   *        must not contain a reference to the <code>monitoredObject</code> or it won't become
   *        phantom-reachable and will never be cleaned
   * @return the registered {@link Cleanable} from {@link Cleaner#register(Object, Runnable)}
   */
  public static Cleanable unclosed(AutoCloseable obj, Class<?> objClass, AtomicBoolean closed,
      Logger log, AutoCloseable closeable) {
    String className = requireNonNull(objClass).getSimpleName();
    requireNonNull(closed);
    requireNonNull(log);
    String closeableClassName = closeable == null ? null : closeable.getClass().getSimpleName();

    // capture the stack trace during setup for logging later, so user can find unclosed object
    var stackTrace = new Exception();

    // register the action to run when obj becomes phantom-reachable or clean is explicitly called
    return CLEANER.register(obj, () -> {
      if (closed.get()) {
        // already closed; nothing to do
        return;
      }
      log.warn("{} found unreferenced without calling close()", className, stackTrace);
      if (closeable != null) {
        try {
          closeable.close();
        } catch (Exception e1) {
          log.error("{} internal error; exception closing {}", objClass, closeableClassName, e1);
        }
      }
    });
  }

  public static Cleanable shutdownThreadPoolExecutor(ExecutorService pool, AtomicBoolean closed,
      Logger log) {
    requireNonNull(pool);
    requireNonNull(log);
    return CLEANER.register(pool, () -> {
      if (closed.get()) {
        return;
      }
      log.warn("{} found unreferenced without calling shutdown() or shutdownNow()",
          pool.getClass().getSimpleName());
      try {
        pool.shutdownNow();
      } catch (Exception e) {
        log.error("internal error; exception closing {}", pool.getClass().getSimpleName(), e);
      }
    });
  }

  // this done for the BatchWriterIterator test code; I don't trust that pattern, but
  // registering a cleaner is something any user is probably going to have to do to clean up
  // resources used in an iterator, until iterators properly implement their own close()
  public static Cleanable batchWriterAndClientCloser(Object o, Logger log, BatchWriter bw,
      AccumuloClient client) {
    requireNonNull(log);
    requireNonNull(bw);
    requireNonNull(client);
    return CLEANER.register(o, () -> {
      try (client) {
        bw.close();
      } catch (MutationsRejectedException e) {
        log.error("Failed to close BatchWriter; some mutations may not be applied", e);
      }
    });
  }

  // this is dubious; MetadataConstraints should probably use the ZooCache provided by context
  // can be done in a follow-on action; for now, this merely replaces the previous finalizer
  public static Cleanable zooCacheClearer(Object o, ZooCache zc) {
    requireNonNull(zc);
    return CLEANER.register(o, zc::clear);
  }

}
