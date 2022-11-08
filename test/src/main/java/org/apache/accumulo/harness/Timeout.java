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
package org.apache.accumulo.harness;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

import org.junit.jupiter.api.extension.DynamicTestInvocationContext;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

public final class Timeout implements InvocationInterceptor {

  private final Duration timeoutDuration;

  public static Timeout from(Supplier<Duration> timeoutDurationSupplier) {
    return new Timeout(timeoutDurationSupplier.get());
  }

  private Timeout(Duration timeoutDuration) {
    this.timeoutDuration = Objects.requireNonNull(timeoutDuration);
  }

  @Override
  public <T> T interceptTestClassConstructor(Invocation<T> invocation,
      ReflectiveInvocationContext<Constructor<T>> invocationContext,
      ExtensionContext extensionContext) {
    return assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptBeforeAllMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptBeforeEachMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptTestMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public <T> T interceptTestFactoryMethod(Invocation<T> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    return assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptTestTemplateMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptDynamicTest(Invocation<Void> invocation,
      DynamicTestInvocationContext invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptAfterEachMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }

  @Override
  public void interceptAfterAllMethod(Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) {
    assertTimeoutPreemptively(timeoutDuration, invocation::proceed);
  }
}
