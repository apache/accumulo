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
package org.apache.accumulo.server.security.handler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.codec.digest.Crypt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZKSecurityToolTest {

  // use a fixed salt instead of random, for testing only; this is just so we can print the hashes
  // for manual comparison when debugging this test
  private final String TEST_SALT = "$6$test$";
  private final String good = "GOODPASSWORD";
  private final String goodHash = Crypt.crypt(good.getBytes(UTF_8), TEST_SALT);

  private final String good2 = "GOODPASSWORD2";
  private final String goodHash2 = Crypt.crypt(good2.getBytes(UTF_8), TEST_SALT);

  private final String bad = "BADPASSWORD";

  @BeforeEach
  public void clearCache() {
    ZKSecurityTool.clearCryptPasswordCache();
  }

  @Test
  public void testCheckCryptPass() {
    assertEquals(0L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertTrue(ZKSecurityTool.checkCryptPass(good.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
    assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertFalse(ZKSecurityTool.checkCryptPass(bad.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
    assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertTrue(ZKSecurityTool.checkCryptPass(good.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
    assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertFalse(ZKSecurityTool.checkCryptPass(bad.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
    assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());

    // test two things in password cache
    assertFalse(ZKSecurityTool.checkCryptPass(bad.getBytes(UTF_8), goodHash2.getBytes(UTF_8)));
    assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertTrue(ZKSecurityTool.checkCryptPass(good2.getBytes(UTF_8), goodHash2.getBytes(UTF_8)));
    assertEquals(2L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertTrue(ZKSecurityTool.checkCryptPass(good2.getBytes(UTF_8), goodHash2.getBytes(UTF_8)));
    assertEquals(2L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertFalse(ZKSecurityTool.checkCryptPass(bad.getBytes(UTF_8), goodHash2.getBytes(UTF_8)));
    assertEquals(2L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertTrue(ZKSecurityTool.checkCryptPass(good.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
    assertEquals(2L, ZKSecurityTool.getCryptPasswordCacheSize());

    // ensure nothing is stored in cache when password does not match what is in ZK
    ZKSecurityTool.clearCryptPasswordCache();
    assertEquals(0L, ZKSecurityTool.getCryptPasswordCacheSize());
    assertFalse(ZKSecurityTool.checkCryptPass(bad.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
    assertEquals(0L, ZKSecurityTool.getCryptPasswordCacheSize());
  }

  @Test
  public void testMatchingPasswordDifferentHash() {
    assertEquals(0L, ZKSecurityTool.getCryptPasswordCacheSize());

    // place any portion of the hash in the password, so it matches the cache key but not the value
    // portion placed in password must be at least length 1, so start index at 1
    for (int index = 1; index <= goodHash.length(); index++) {
      assertTrue(ZKSecurityTool.checkCryptPass(good.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
      assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());
      String passwordPlusHash = good + goodHash.substring(0, index);
      assertFalse(ZKSecurityTool.checkCryptPass(passwordPlusHash.getBytes(UTF_8),
          goodHash.substring(index, goodHash.length()).getBytes(UTF_8)));
      // This invalidates the existing key because the cache key matches but the hashes do not
      assertEquals(0L, ZKSecurityTool.getCryptPasswordCacheSize());

      assertTrue(ZKSecurityTool.checkCryptPass(good.getBytes(UTF_8), goodHash.getBytes(UTF_8)));
      assertEquals(1L, ZKSecurityTool.getCryptPasswordCacheSize());
    }
  }
}
