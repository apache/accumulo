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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Version {
  String package_ = null;
  int major = 0;
  int minor = 0;
  int release = 0;
  String etcetera = null;

  public Version(String everything) {
    parse(everything);
  }

  private void parse(String everything) {
    Pattern pattern = Pattern.compile("(([^-]*)-)?(\\d+)(\\.(\\d+)(\\.(\\d+))?)?(-(.*))?");
    Matcher parser = pattern.matcher(everything);
    if (!parser.matches())
      throw new IllegalArgumentException("Unable to parse: " + everything + " as a version");

    if (parser.group(1) != null)
      package_ = parser.group(2);
    major = Integer.valueOf(parser.group(3));
    minor = 0;
    if (parser.group(5) != null)
      minor = Integer.valueOf(parser.group(5));
    if (parser.group(7) != null)
      release = Integer.valueOf(parser.group(7));
    if (parser.group(9) != null)
      etcetera = parser.group(9);

  }

  public String getPackage() {
    return package_;
  }

  public int getMajorVersion() {
    return major;
  }

  public int getMinorVersion() {
    return minor;
  }

  public int getReleaseVersion() {
    return release;
  }

  public String getEtcetera() {
    return etcetera;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (package_ != null) {
      result.append(package_);
      result.append("-");
    }
    result.append(major);
    result.append(".");
    result.append(minor);
    result.append(".");
    result.append(release);
    if (etcetera != null) {
      result.append("-");
      result.append(etcetera);
    }
    return result.toString();
  }

}
