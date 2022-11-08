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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidatorTest {

  private Validator<String> v, v2, v3;
  private static final Pattern STARTSWITH_C = Pattern.compile("c.*");

  @BeforeEach
  public void setUp() {
    v = new Validator<>(
        arg -> "correct".equals(arg) ? Validator.OK : Optional.of("Invalid argument " + arg));
    v2 = new Validator<>(arg -> "righto".equals(arg) ? Validator.OK
        : Optional.of("Not a correct argument : " + arg + " : done"));
    v3 = new Validator<>(s -> s != null && STARTSWITH_C.matcher(s).matches() ? Validator.OK
        : Optional.of("Invalid argument " + s));
  }

  @Test
  public void testValidate_Success() {
    assertEquals("correct", v.validate("correct"));
  }

  @Test
  public void testValidate_Failure() {
    // check default message maker
    var e = assertThrows(IllegalArgumentException.class, () -> v.validate("incorrect"));
    assertEquals("Invalid argument incorrect", e.getMessage());
    // check custom message maker
    e = assertThrows(IllegalArgumentException.class, () -> v2.validate("somethingwrong"));
    assertEquals("Not a correct argument : somethingwrong : done", e.getMessage());
  }

  @Test
  public void testAnd() {
    Validator<String> vand = v3.and(v);
    assertEquals("correct", vand.validate("correct"));
    assertThrows(IllegalArgumentException.class, () -> vand.validate("righto"));
    assertThrows(IllegalArgumentException.class, () -> vand.validate("coriander"));
  }

  @Test
  public void testOr() {
    Validator<String> vor = v.or(v2);
    assertEquals("correct", vor.validate("correct"));
    assertEquals("righto", vor.validate("righto"));
    assertThrows(IllegalArgumentException.class, () -> vor.validate("coriander"));
  }

  @Test
  public void testNot() {
    Validator<String> vnot = v3.not();
    var e = assertThrows(IllegalArgumentException.class, () -> vnot.validate("correct"));
    assertEquals("Validation should have failed with: Invalid argument correct", e.getMessage());
    e = assertThrows(IllegalArgumentException.class, () -> vnot.validate("coriander"));
    assertEquals("Validation should have failed with: Invalid argument coriander", e.getMessage());
    assertEquals("righto", vnot.validate("righto"));
    assertEquals("anythingNotStartingWithLowercaseC",
        vnot.validate("anythingNotStartingWithLowercaseC"));
  }
}
