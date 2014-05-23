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

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;

public class DefaultConfiguration extends AccumuloConfiguration {
  private static DefaultConfiguration instance = null;
  private Map<String,String> resolvedProps = null;

  synchronized public static DefaultConfiguration getInstance() {
    if (instance == null) {
      instance = new DefaultConfiguration();
      ConfigSanityCheck.validate(instance);
    }
    return instance;
  }

  @Override
  public String get(Property property) {
    return getResolvedProps().get(property.getKey());
  }

  private synchronized Map<String,String> getResolvedProps() {
    if (resolvedProps == null) {
      // the following loop is super slow, it takes a few milliseconds, so cache it
      resolvedProps = new HashMap<String,String>();
      for (Property prop : Property.values())
        if (!prop.getType().equals(PropertyType.PREFIX))
          resolvedProps.put(prop.getKey(), prop.getDefaultValue());
    }
    return resolvedProps;
  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    for (Entry<String,String> entry : getResolvedProps().entrySet())
      if (filter.accept(entry.getKey()))
        props.put(entry.getKey(), entry.getValue());
  }

  /*
   * Generate documentation for conf/accumulo-site.xml file usage
   */
  public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
    if (args.length == 2 && args[0].equals("--generate-html")) {
      new ConfigurationDocGen(new PrintStream(args[1], Constants.UTF8.name())).generateHtml();
    } else if (args.length == 2 && args[0].equals("--generate-latex")) {
      new ConfigurationDocGen(new PrintStream(args[1], Constants.UTF8.name())).generateLaTeX();
    } else {
      throw new IllegalArgumentException("Usage: " + DefaultConfiguration.class.getName() + " --generate-html <filename> | --generate-latex <filename>");
    }
  }

}
