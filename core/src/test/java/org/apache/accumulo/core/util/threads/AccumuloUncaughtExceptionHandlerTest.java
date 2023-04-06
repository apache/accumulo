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
package org.apache.accumulo.core.util.threads;

import static org.apache.accumulo.core.util.threads.AccumuloUncaughtExceptionHandler.isError;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.jupiter.api.Test;

public class AccumuloUncaughtExceptionHandlerTest {

  @Test
  public void testIsError_noerror() {
    assertFalse(isError(new IOException()));
    assertFalse(isError(new UncheckedIOException(new IOException())));

    Exception e = new UncheckedIOException(new IOException());
    e.addSuppressed(new RuntimeException());
    e.addSuppressed(new RuntimeException());
    assertFalse(isError(e));
  }

  @Test
  public void testIsError_error() {
    assertTrue(isError(new UnsatisfiedLinkError()));
    assertTrue(isError(new RuntimeException(new OutOfMemoryError())));
    assertTrue(isError(new RuntimeException(new RuntimeException(new UnsatisfiedLinkError()))));

    // check for cases where error has a non-error cause
    assertTrue(isError(new Error(new RuntimeException())));
    assertTrue(isError(new RuntimeException(new Error(new RuntimeException()))));
    assertTrue(
        isError(new RuntimeException(new RuntimeException(new Error(new RuntimeException())))));

    // check for suppressed exception that has error
    Exception e = new UncheckedIOException(new IOException());
    e.addSuppressed(new RuntimeException());
    e.addSuppressed(new RuntimeException());
    e.addSuppressed(new RuntimeException(new UnsatisfiedLinkError()));
    assertTrue(isError(e));
    assertTrue(isError(new RuntimeException(e)));

    // check for suppressed exception that has non terminal error
    Exception e2 = new UncheckedIOException(new IOException());
    e2.addSuppressed(new RuntimeException());
    e2.addSuppressed(new RuntimeException());
    e2.addSuppressed(new RuntimeException(new Error(new RuntimeException())));
    assertTrue(isError(e2));
    assertTrue(isError(new RuntimeException(e2)));

    // test suppressed with error a few levels deep
    Exception ed1 = new UncheckedIOException(new IOException());
    Exception ed2 = new UncheckedIOException(new IOException());
    Exception ed3 = new RuntimeException(new OutOfMemoryError());
    ed1.addSuppressed(ed2);
    ed2.addSuppressed(ed3);
    assertTrue(isError(ed1));
    assertTrue(isError(new RuntimeException(ed1)));

    // test case where suppressed is an error
    Exception e4 = new UncheckedIOException(new IOException());
    e4.addSuppressed(new RuntimeException());
    e4.addSuppressed(new RuntimeException());
    e4.addSuppressed(new Error(new RuntimeException())); // try direct error (not nested as cause)
    assertTrue(isError(e4));
    assertTrue(isError(new RuntimeException(e4)));
  }

  @Test
  public void testIsError_loop() {
    Exception e1 = new UncheckedIOException(new IOException());
    Exception e2 = new RuntimeException(new RuntimeException());
    Exception e3 = new IllegalStateException();

    // create a chain of suppressed exceptions that forms a loop
    e1.addSuppressed(e2);
    e2.addSuppressed(e3);
    e3.addSuppressed(e1);

    assertFalse(isError(e1));
    assertFalse(isError(new RuntimeException(e1)));
  }
}
