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

import org.apache.commons.codec.binary.StringUtils;

/**
 * A wrapper around commons-codec's Base64 to make sure we get the non-chunked behavior that became the default in commons-codec version 1.5+ while relying on
 * the commons-codec version 1.4 that Hadoop Client provides.
 */
public final class Base64 {

  /**
   * Private to prevent instantiation.
   */
  private Base64() {}

  /**
   * Serialize to Base64 byte array, non-chunked.
   */
  public static byte[] encodeBase64(byte[] data) {
    return org.apache.commons.codec.binary.Base64.encodeBase64(data, false);
  }

  /**
   * Serialize to Base64 as a String, non-chunked.
   */
  public static String encodeBase64String(byte[] data) {
    /* Based on implementation of this same name function in commons-codec 1.5+. in commons-codec 1.4, the second param sets chunking to true. */
    return StringUtils.newStringUtf8(org.apache.commons.codec.binary.Base64.encodeBase64(data, false));
  }

  /**
   * Serialize to Base64 as a String using the URLSafe alphabet, non-chunked.
   *
   * The URLSafe alphabet uses - instead of + and _ instead of /.
   */
  public static String encodeBase64URLSafeString(byte[] data) {
    return org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(data);
  }

  /**
   * Decode, presuming bytes are base64.
   *
   * Transparently handles either the standard alphabet or the URL Safe one.
   */
  public static byte[] decodeBase64(byte[] base64) {
    return org.apache.commons.codec.binary.Base64.decodeBase64(base64);
  }

  /**
   * Decode, presuming String is base64.
   *
   * Transparently handles either the standard alphabet or the URL Safe one.
   */
  public static byte[] decodeBase64(String base64String) {
    return org.apache.commons.codec.binary.Base64.decodeBase64(base64String);
  }
}
