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
package org.apache.accumulo.fate.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressUtil {

  private static final Logger log = LoggerFactory.getLogger(AddressUtil.class);

  /**
   * Fetch the security value that determines how long DNS failures are cached. Looks up the security property 'networkaddress.cache.negative.ttl'. Should that
   * fail returns the default value used in the Oracle JVM 1.4+, which is 10 seconds.
   *
   * @param originalException
   *          the host lookup that is the source of needing this lookup. maybe be null.
   * @return positive integer number of seconds
   * @see InetAddress
   * @throws IllegalArgumentException
   *           if dns failures are cached forever
   */
  static public int getAddressCacheNegativeTtl(UnknownHostException originalException) {
    int negativeTtl = 10;
    try {
      negativeTtl = Integer.parseInt(Security.getProperty("networkaddress.cache.negative.ttl"));
    } catch (NumberFormatException exception) {
      log.warn("Failed to get JVM negative DNS respones cache TTL due to format problem (e.g. this JVM might not have the "
          + "property). Falling back to default based on Oracle JVM 1.4+ (10s)", exception);
    } catch (SecurityException exception) {
      log.warn("Failed to get JVM negative DNS response cache TTL due to security manager. Falling back to default based on Oracle JVM 1.4+ (10s)", exception);
    }
    if (-1 == negativeTtl) {
      log.error("JVM negative DNS repsonse cache TTL is set to 'forever' and host lookup failed. TTL can be changed with security property "
          + "'networkaddress.cache.negative.ttl', see java.net.InetAddress.", originalException);
      throw new IllegalArgumentException(originalException);
    } else if (0 > negativeTtl) {
      log.warn("JVM specified negative DNS response cache TTL was negative (and not 'forever'). Falling back to default based on Oracle JVM 1.4+ (10s)");
      negativeTtl = 10;
    }
    return negativeTtl;
  }

}
