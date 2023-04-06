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
package org.apache.accumulo.monitor.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Basic tests for ParameterValidator
 */
public class ParameterValidatorTest {

  @Test
  public void testAlphaNumRegex() {
    Pattern p = Pattern.compile(ParameterValidator.ALPHA_NUM_REGEX);
    assertTrue(p.matcher("asdlkfj234kj324").matches());
    assertFalse(p.matcher("234-324").matches());
    assertFalse(p.matcher("").matches());

    p = Pattern.compile(ParameterValidator.ALPHA_NUM_REGEX_BLANK_OK);
    assertTrue(p.matcher("asdlkfj234kj324").matches());
    assertTrue(p.matcher("").matches());
    assertFalse(p.matcher("234-324").matches());
  }

  @Test
  public void testServerRegex() {
    Pattern p = Pattern.compile(ParameterValidator.HOSTNAME_PORT_REGEX);
    assertTrue(p.matcher("ab3cd.12d34.3xyz.net:12").matches(), "Did not match hostname with dots");
    assertTrue(p.matcher("abcd.123.server-foo.com:56789").matches(),
        "Did not match hostname with dash");
    assertTrue(p.matcher("abcd.123.server-foo.com:1234").matches(),
        "Did not match hostname and port");
    assertTrue(p.matcher("127.0.0.1:9999").matches(), "Did not match all numeric and port");
    assertTrue(p.matcher("ServerName:9999").matches(), "Did not match all numeric and port");

    assertFalse(p.matcher("127.0.0.1").matches(), "Port number required");
    assertFalse(p.matcher("abcd.1234.*.xyz").matches());
    assertFalse(p.matcher("abcd.1234.;xyz").matches());
    assertFalse(p.matcher("abcd.12{3}4.xyz").matches());
    assertFalse(p.matcher("abcd.12[3]4.xyz").matches());
    assertFalse(p.matcher("abcd=4.xyz").matches());
    assertFalse(p.matcher("abcd=\"4.xyz\"").matches());
    assertFalse(p.matcher("abcd\"4.xyz\"").matches());
  }

}
