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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

public class DefaultConfiguration extends AccumuloConfiguration {
  private static DefaultConfiguration instance = null;
  private static Logger log = Logger.getLogger(DefaultConfiguration.class);
  
  synchronized public static DefaultConfiguration getInstance() {
    if (instance == null) {
      instance = new DefaultConfiguration();
      ConfigSanityCheck.validate(instance);
    }
    return instance;
  }
  
  @Override
  public String get(Property property) {
    return property.getDefaultValue();
  }
  
  @Override
  public Iterator<Entry<String,String>> iterator() {
    TreeMap<String,String> entries = new TreeMap<String,String>();
    for (Property prop : Property.values())
      if (!prop.isExperimental() && !prop.getType().equals(PropertyType.PREFIX))
        entries.put(prop.getKey(), prop.getDefaultValue());
    
    return entries.entrySet().iterator();
  }
  
  private static void generateDocumentation(PrintStream doc) {
    // read static content from resources and output
    InputStream data = DefaultConfiguration.class.getResourceAsStream("config.html");
    if (data != null) {
      byte[] buffer = new byte[1024];
      int n;
      try {
        while ((n = data.read(buffer)) > 0)
          doc.print(new String(buffer, 0, n));
      } catch (IOException e) {
        e.printStackTrace();
        return;
      } finally {
        try {
          data.close();
        } catch (IOException ex) {
          log.error(ex, ex);
        }
      }
    }
    doc.println();
    
    ArrayList<Property> prefixes = new ArrayList<Property>();
    TreeMap<String,Property> sortedProps = new TreeMap<String,Property>();
    for (Property prop : Property.values()) {
      if (prop.isExperimental())
        continue;
      
      if (prop.getType().equals(PropertyType.PREFIX))
        prefixes.add(prop);
      else
        sortedProps.put(prop.getKey(), prop);
    }
    
    int indentDepth = 2;
    doc.println(indent(indentDepth++) + "<p>Jump to: ");
    String delimiter = "";
    for (Property prefix : prefixes) {
      if (prefix.isExperimental())
        continue;
      
      doc.print(delimiter + "<a href='#" + prefix.name() + "'>" + prefix.getKey() + "*</a>");
      delimiter = "&nbsp;|&nbsp;";
    }
    doc.println(indent(--indentDepth) + "</p>");
    
    doc.println(indent(indentDepth++) + "<table>");
    for (Property prefix : prefixes) {
      
      if (prefix.isExperimental())
        continue;
      
      boolean isDeprecated = prefix.isDeprecated();
      
      doc.println(indent(indentDepth) + "<tr><td colspan='5'" + (isDeprecated ? " class='deprecated'" : "") + "><a id='" + prefix.name() + "' class='large'>"
          + prefix.getKey() + "*</a></td></tr>");
      doc.println(indent(indentDepth) + "<tr><td colspan='5'" + (isDeprecated ? " class='deprecated'" : "") + "><i>"
          + (isDeprecated ? "<b><i>Deprecated.</i></b> " : "") + prefix.getDescription() + "</i></td></tr>");
      if (!prefix.equals(Property.TABLE_CONSTRAINT_PREFIX) && !prefix.equals(Property.TABLE_ITERATOR_PREFIX)
          && !prefix.equals(Property.TABLE_LOCALITY_GROUP_PREFIX))
        doc.println(indent(indentDepth) + "<tr><th>Property</th><th>Type</th><th>Zookeeper Mutable</th><th>Default Value</th><th>Description</th></tr>");
      
      boolean highlight = true;
      for (Property prop : sortedProps.values()) {
        if (prop.isExperimental())
          continue;
        
        isDeprecated = prefix.isDeprecated() || prop.isDeprecated();
        
        if (prop.getKey().startsWith(prefix.getKey())) {
          doc.println(indent(indentDepth++) + "<tr" + (highlight ? " class='highlight'" : "") + ">");
          highlight = !highlight;
          doc.println(indent(indentDepth) + "<td" + (isDeprecated ? " class='deprecated'" : "") + ">" + prop.getKey() + "</td>");
          doc.println(indent(indentDepth) + "<td" + (isDeprecated ? " class='deprecated'" : "") + "><b><a href='#" + prop.getType().name() + "'>"
              + prop.getType().toString().replaceAll(" ", "&nbsp;") + "</a></b></td>");
          String zoo = "no";
          if (Property.isValidZooPropertyKey(prop.getKey())) {
            zoo = "yes";
            if (Property.isFixedZooPropertyKey(prop)) {
              zoo = "yes but requires restart of the " + prop.getKey().split("[.]")[0];
            }
          }
          doc.println(indent(indentDepth) + "<td" + (isDeprecated ? " class='deprecated'" : "") + ">" + zoo + "</td>");
          doc.println(indent(indentDepth) + "<td" + (isDeprecated ? " class='deprecated'" : "") + "><pre>"
              + (prop.getDefaultValue().isEmpty() ? "&nbsp;" : prop.getDefaultValue().replaceAll(" ", "&nbsp;")) + "</pre></td>");
          doc.println(indent(indentDepth) + "<td" + (isDeprecated ? " class='deprecated'" : "") + ">" + (isDeprecated ? "<b><i>Deprecated.</i></b> " : "")
              + prop.getDescription() + "</td>");
          doc.println(indent(--indentDepth) + "</tr>");
        }
      }
    }
    doc.println(indent(--indentDepth) + "</table>");
    
    doc.println(indent(indentDepth) + "<h1>Property Type Descriptions</h1>");
    doc.println(indent(indentDepth++) + "<table>");
    doc.println(indent(indentDepth) + "<tr><th>Property Type</th><th>Description</th></tr>");
    boolean highlight = true;
    for (PropertyType type : PropertyType.values()) {
      if (type.equals(PropertyType.PREFIX))
        continue;
      doc.println(indent(indentDepth++) + "<tr " + (highlight ? "class='highlight'" : "") + ">");
      highlight = !highlight;
      doc.println(indent(indentDepth) + "<td><h3><a id='" + type.name() + "'>" + type + "</a></h3></td>");
      doc.println(indent(indentDepth) + "<td>" + type.getFormatDescription() + "</td>");
      doc.println(indent(--indentDepth) + "</tr>");
    }
    doc.println(indent(--indentDepth) + "</table>");
    doc.println(indent(--indentDepth) + "</body>");
    doc.println(indent(--indentDepth) + "</html>");
    doc.close();
  }
  
  private static String indent(int depth) {
    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < depth; d++)
      sb.append(" ");
    return sb.toString();
  }
  
  /*
   * Generate documentation for conf/accumulo-site.xml file usage
   */
  public static void main(String[] args) throws FileNotFoundException {
    if (args.length == 2 && args[0].equals("--generate-doc")) {
      generateDocumentation(new PrintStream(args[1]));
    } else {
      throw new IllegalArgumentException("Usage: " + DefaultConfiguration.class.getName() + " --generate-doc <filename>");
    }
  }
  
}
