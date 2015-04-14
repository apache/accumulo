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
package org.apache.accumulo.core.conf;

import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for validating {@link AccumuloConfiguration} instances.
 */
public class ConfigSanityCheck {

  private static final Logger log = LoggerFactory.getLogger(ConfigSanityCheck.class);
  private static final String PREFIX = "BAD CONFIG ";
  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_URI = Property.INSTANCE_DFS_URI;
  @SuppressWarnings("deprecation")
  private static final Property INSTANCE_DFS_DIR = Property.INSTANCE_DFS_DIR;

  /**
   * Validates the given configuration entries. A valid configuration contains only valid properties (i.e., defined or otherwise valid) that are not prefixes
   * and whose values are formatted correctly for their property types. A valid configuration also contains a value for property
   * {@link Property#INSTANCE_ZK_TIMEOUT} within a valid range.
   *
   * @param entries
   *          iterable through configuration keys and values
   * @throws SanityCheckException
   *           if a fatal configuration error is found
   */
  public static void validate(Iterable<Entry<String,String>> entries) {
    String instanceZkTimeoutValue = null;
    boolean usingVolumes = false;
    for (Entry<String,String> entry : entries) {
      String key = entry.getKey();
      String value = entry.getValue();
      Property prop = Property.getPropertyByKey(entry.getKey());
      if (prop == null && Property.isValidPropertyKey(key))
        continue; // unknown valid property (i.e. has proper prefix)
      else if (prop == null)
        log.warn("{} unrecognized property key ({})", PREFIX , key);
      else if (prop.getType() == PropertyType.PREFIX) {
        log.error("{} incomplete property key ({})", PREFIX, key);
        throw new SanityCheckException(String.format("%s incomplete property key (%s)", PREFIX, key));
      }
      else if (!prop.getType().isValidFormat(value)) {
        log.error("{} improperly formatted value for key ({}, type={})", PREFIX, key, prop.getType());
        throw new SanityCheckException(String.format("%s improperly formatted value for key (%s, type=%s)", PREFIX, key, prop.getType()));
      }

      if (key.equals(Property.INSTANCE_ZK_TIMEOUT.getKey())) {
        instanceZkTimeoutValue = value;
      }

      if (key.equals(Property.INSTANCE_VOLUMES.getKey())) {
        usingVolumes = value != null && !value.isEmpty();
      }
    }

    if (instanceZkTimeoutValue != null) {
      checkTimeDuration(Property.INSTANCE_ZK_TIMEOUT, instanceZkTimeoutValue, new CheckTimeDurationBetween(1000, 300000));
    }

    if (!usingVolumes) {
      log.warn("Use of {} and {} are deprecated. Consider using {} instead.", INSTANCE_DFS_URI, INSTANCE_DFS_DIR, Property.INSTANCE_VOLUMES);
    }
  }

  private interface CheckTimeDuration {
    boolean check(long propVal);

    String getDescription(Property prop);
  }

  private static class CheckTimeDurationBetween implements CheckTimeDuration {
    long min, max;

    CheckTimeDurationBetween(long x, long y) {
      min = Math.min(x, y);
      max = Math.max(x, y);
    }

    @Override
    public boolean check(long propVal) {
      return propVal >= min && propVal <= max;
    }

    @Override
    public String getDescription(Property prop) {
      return "ensure " + min + " <= " + prop + " <= " + max;
    }
  }

  private static void checkTimeDuration(Property prop, String value, CheckTimeDuration chk) {
    verifyPropertyTypes(PropertyType.TIMEDURATION, prop);
    if (!chk.check(AccumuloConfiguration.getTimeInMillis(value))) {
      log.error("{}{}", PREFIX, chk.getDescription(prop));
      throw new SanityCheckException(String.format("%s%s", PREFIX, chk.getDescription(prop)));
    }
  }

  private static void verifyPropertyTypes(PropertyType type, Property... properties) {
    for (Property prop : properties)
      if (prop.getType() != type) {
        log.error("FATAL: Unexpected property type ({} != {})", prop.getType(), type);
        throw new SanityCheckException(String.format("Unexpected property type (%s != %s)", prop.getType(), type));
      }
  }

  /**
   * The exception thrown when {@link ConfigSanityCheck#validate(Iterable)} fails.
   */
  public static class SanityCheckException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new exception with the given message.
     */
    public SanityCheckException(String msg) {
      super(msg);
    }
  }
}
