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

import org.apache.log4j.Logger;

public class ConfigSanityCheck {
  
  private static final Logger log = Logger.getLogger(ConfigSanityCheck.class);
  private static final String PREFIX = "BAD CONFIG ";
  
  public static void validate(AccumuloConfiguration acuconf) {
    for (Entry<String,String> entry : acuconf) {
      String key = entry.getKey();
      String value = entry.getValue();
      Property prop = Property.getPropertyByKey(entry.getKey());
      if (prop == null && Property.isValidTablePropertyKey(key))
        continue; // unknown valid per-table property
      else if (prop == null)
        log.warn(PREFIX + "unrecognized property key (" + key + ")");
      else if (prop.getType() == PropertyType.PREFIX)
        fatal(PREFIX + "incomplete property key (" + key + ")");
      else if (!prop.getType().isValidFormat(value))
        fatal(PREFIX + "improperly formatted value for key (" + key + ", type=" + prop.getType() + ")");
    }
    
    checkTimeDuration(acuconf, Property.INSTANCE_ZK_TIMEOUT, new CheckTimeDurationBetween(1000, 300000));
  }
  
  private static interface CheckTimeDuration {
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
  
  private static void checkTimeDuration(AccumuloConfiguration acuconf, Property prop, CheckTimeDuration chk) {
    verifyPropertyTypes(PropertyType.TIMEDURATION, prop);
    if (!chk.check(acuconf.getTimeInMillis(prop)))
      fatal(PREFIX + chk.getDescription(prop));
  }
  
  private static void verifyPropertyTypes(PropertyType type, Property... properties) {
    for (Property prop : properties)
      if (prop.getType() != type)
        fatal("Unexpected property type (" + prop.getType() + " != " + type + ")");
  }
  
  private static void fatal(String msg) {
    log.fatal(msg);
    throw new RuntimeException(msg);
  }
}
