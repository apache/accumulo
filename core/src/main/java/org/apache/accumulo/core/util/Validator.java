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
package org.apache.accumulo.core.util;

import java.util.function.Predicate;

/**
 * A class that validates arguments of a particular type. Implementations must implement {@link #test(Object)} and should override
 * {@link #invalidMessage(Object)}.
 */
public abstract class Validator<T> implements Predicate<T> {

  /**
   * Validates an argument.
   *
   * @param argument
   *          argument to validate
   * @return the argument, if validation passes
   * @throws IllegalArgumentException
   *           if validation fails
   */
  public final T validate(final T argument) {
    if (!test(argument))
      throw new IllegalArgumentException(invalidMessage(argument));
    return argument;
  }

  /**
   * Formulates an exception message for invalid values.
   *
   * @param argument
   *          argument that failed validation
   * @return exception message
   */
  public String invalidMessage(final T argument) {
    return String.format("Invalid argument %s", argument);
  }

  /**
   * Creates a new validator that is the conjunction of this one and the given one. An argument passed to the returned validator is valid only if it passes both
   * validators.
   *
   * @param other
   *          other validator
   * @return combined validator
   */
  public final Validator<T> and(final Validator<T> other) {
    if (other == null)
      return this;
    final Validator<T> mine = this;
    return new Validator<T>() {

      @Override
      public boolean test(T argument) {
        return mine.test(argument) && other.test(argument);
      }

      @Override
      public String invalidMessage(T argument) {
        return (mine.test(argument) ? other : mine).invalidMessage(argument);
      }

    };
  }

  /**
   * Creates a new validator that is the disjunction of this one and the given one. An argument passed to the returned validator is valid only if it passes at
   * least one of the validators.
   *
   * @param other
   *          other validator
   * @return combined validator
   */
  public final Validator<T> or(final Validator<T> other) {
    if (other == null)
      return this;
    final Validator<T> mine = this;
    return new Validator<T>() {

      @Override
      public boolean test(T argument) {
        return mine.test(argument) || other.test(argument);
      }

      @Override
      public String invalidMessage(T argument) {
        return mine.invalidMessage(argument);
      }

    };
  }

  /**
   * Creates a new validator that is the negation of this one. An argument passed to the returned validator is valid only if it fails this one.
   *
   * @return negated validator
   */
  public final Validator<T> not() {
    final Validator<T> mine = this;
    return new Validator<T>() {

      @Override
      public boolean test(T argument) {
        return !mine.test(argument);
      }

      @Override
      public String invalidMessage(T argument) {
        return "Validation should have failed with: " + mine.invalidMessage(argument);
      }

    };
  }
}
