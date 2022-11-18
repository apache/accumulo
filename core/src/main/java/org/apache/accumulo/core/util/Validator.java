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
package org.apache.accumulo.core.util;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * A class that validates arguments of a specified generic type. Given a validation function that
 * emits an error message if and only if the validation fails, this object's validate method will
 * return the original valid argument, or throw an IllegalArgumentException with the custom error
 * message if it fails to validate.
 */
public class Validator<T> {

  public static final Optional<String> OK = Optional.empty();

  private final Function<T,Optional<String>> validateFunction;

  private volatile T lastValidated = null;

  /**
   * Constructor to build a validator given the mapping function that validates. If the argument is
   * valid, the mapping function should return an empty Optional. Otherwise, it should return an
   * Optional containing the error message to be set in the IllegalArgumentException.
   *
   * @param validateFunction the function that validates or returns an error message
   */
  public Validator(final Function<T,Optional<String>> validateFunction) {
    this.validateFunction = requireNonNull(validateFunction);
  }

  /**
   * Validates the provided argument.
   *
   * @param argument argument to validate
   * @return the argument, if validation passes
   * @throws IllegalArgumentException if validation fails
   */
  public final T validate(final T argument) {
    // check if argument was recently validated, to short-circuit the check
    // this especially helps if an API validates, then calls another API that validates the same
    T lastValidatedSnapshot = lastValidated;
    if (lastValidatedSnapshot != null && Objects.equals(argument, lastValidatedSnapshot)) {
      return argument;
    }

    validateFunction.apply(argument).ifPresent(msg -> {
      throw new IllegalArgumentException(msg);
    });

    // save most recently validated, to save time validating again
    lastValidated = argument;
    return argument;
  }

  /**
   * Creates a new validator that is the conjunction of this one and the given one. An argument
   * passed to the returned validator is valid if only if it passes both validators. If the other
   * validator is null, the current validator is returned unchanged.
   *
   * @param other other validator
   * @return combined validator
   */
  public final Validator<T> and(final Validator<T> other) {
    if (other == null) {
      return this;
    }
    return new Validator<>(
        arg -> validateFunction.apply(arg).or(() -> other.validateFunction.apply(arg)));
  }

  /**
   * Creates a new validator that is the disjunction of this one and the given one. An argument
   * passed to the returned validator is valid if and only if it passes at least one of the
   * validators. If the other validator is null, the current validator is returned unchanged.
   *
   * @param other other validator
   * @return combined validator
   */
  public final Validator<T> or(final Validator<T> other) {
    if (other == null) {
      return this;
    }
    return new Validator<>(
        arg -> validateFunction.apply(arg).isEmpty() ? OK : other.validateFunction.apply(arg));
  }

  /**
   * Creates a new validator that is the negation of this one. An argument passed to the returned
   * validator is valid only if it fails this one.
   *
   * @return negated validator
   */
  public final Validator<T> not() {
    return new Validator<>(arg -> {
      return validateFunction.apply(arg).isPresent() ? OK
          : Optional.of("Validation should have failed with: Invalid argument " + arg);
    });
  }

}
