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
package org.apache.accumulo.core.conf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.PrintStream;
import java.util.TreeMap;

/**
 * This class generates documentation to inform users of the available configuration properties in a
 * presentable form.
 */
public class ConfigurationDocGen {
  private PrintStream doc;
  private final TreeMap<String,Property> sortedProps = new TreeMap<>();

  void generate() {
    pageHeader();

    beginTable("Property");
    for (Property prop : sortedProps.values()) {
      if (prop.getType() == PropertyType.PREFIX) {
        prefixSection(prop);
      } else {
        property(prop);
      }
    }

    beginSection("Property Types");
    beginTable("Type");
    propertyTypeDescriptions();

    doc.close();
  }

  void beginSection(String section) {
    doc.println("\n### " + section + "\n");
  }

  void beginTable(String name) {
    doc.println("| " + name + " | Description |");
    doc.println("|--------------|-------------|");
  }

  void pageHeader() {
    doc.println("---");
    doc.println("title: Server Properties (3.x)");
    doc.println("category: configuration");
    doc.println("order: 6");
    doc.println("---\n");
    doc.println("<!-- WARNING: Do not edit this file. It is a generated file"
        + " that is copied from Accumulo build (from core/target/generated-docs) -->\n");
    doc.println("Below are properties set in `accumulo.properties` or the"
        + " Accumulo shell that configure Accumulo servers (i.e. tablet server,"
        + " manager, etc). Properties labeled 'Experimental' should not be considered stable"
        + " and have a higher risk of changing in the future.\n");
  }

  void prefixSection(Property prefix) {
    boolean depr = prefix.isDeprecated();
    String key = strike("<a name=\"" + prefix.getKey().replace(".", "_")
        + "prefix\" class=\"prop\"></a> **" + prefix.getKey() + "***", depr);
    String description = prefix.isExperimental() ? "**Experimental**<br>" : "";
    description += "**Available since:** " + prefix.availableSince() + "<br>";
    if (depr) {
      description += "*Deprecated since:* " + prefix.deprecatedSince() + "<br>";
      if (prefix.isReplaced()) {
        description += "*Replaced by:* <a href=\"#" + prefix.replacedBy().getKey().replace(".", "_")
            + "prefix\">" + prefix.replacedBy() + "</a><br>";
      }
    }
    description += strike(sanitize(prefix.getDescription()), depr);
    doc.println("| " + key + " | " + description + " |");
  }

  void property(Property prop) {
    boolean depr = prop.isDeprecated();
    String key = strike(
        "<a name=\"" + prop.getKey().replace(".", "_") + "\" class=\"prop\"></a> " + prop.getKey(),
        depr);
    String description = prop.isExperimental() ? "**Experimental**<br>" : "";
    description +=
        prop.isExample() ? "**Example: property is not set in default configuration**<br>" : "";
    description += "**Available since:** ";
    if (prop.getKey().startsWith("manager.")
        && (prop.availableSince().startsWith("1.") || prop.availableSince().startsWith("2.0"))) {
      description += "2.1.0 (formerly *master." + prop.getKey().substring(8) + "* since "
          + prop.availableSince() + ")<br>";
    } else {
      description += prop.availableSince() + "<br>";
    }
    if (depr) {
      description += "*Deprecated since:* " + prop.deprecatedSince() + "<br>";
      if (prop.isReplaced()) {
        description += "*Replaced by:* <a href=\"#" + prop.replacedBy().getKey().replace(".", "_")
            + "\">" + prop.replacedBy() + "</a><br>";
      }
    }
    description += strike(sanitize(prop.getDescription()), depr) + "<br>"
        + strike("**type:** " + prop.getType().name(), depr) + ", "
        + strike("**zk mutable:** " + isZooKeeperMutable(prop), depr) + ", ";
    String value, name;
    if (prop.isExample()) {
      value = sanitize(prop.getExampleValue()).trim();
      name = "**example value:** ";
    } else {
      value = sanitize(prop.getDefaultValue()).trim();
      name = "**default value:** ";
    }
    if (value.isEmpty()) {
      description += strike(name + "empty", depr);
    } else if (value.contains("\n")) {
      // deal with multi-line values, skip strikethrough of value
      description += strike(name, depr) + "\n```\n" + value + "\n```\n";
    } else if (prop.getType() == PropertyType.CLASSNAME
        && value.startsWith("org.apache.accumulo")) {
      description += strike(name + "{% jlink -f " + value + " %}", depr);
    } else {
      description += strike(name + "`" + value + "`", depr);
    }
    doc.println("| " + key + " | " + description + " |");
  }

  private String strike(String s, boolean isDeprecated) {
    return (isDeprecated ? "~~" : "") + s + (isDeprecated ? "~~" : "");
  }

  void propertyTypeDescriptions() {
    for (PropertyType type : PropertyType.values()) {
      if (type == PropertyType.PREFIX) {
        continue;
      }
      doc.println(
          "| " + sanitize(type.toString()) + " | " + sanitize(type.getFormatDescription()) + " |");
    }
  }

  String sanitize(String str) {
    return str.replace("\n", "<br>");
  }

  private ConfigurationDocGen(PrintStream doc) {
    this.doc = doc;
    for (Property prop : Property.values()) {
      this.sortedProps.put(prop.getKey(), prop);
    }
  }

  private String isZooKeeperMutable(Property prop) {
    if (!Property.isValidZooPropertyKey(prop.getKey())) {
      return "no";
    }
    if (Property.isFixedZooPropertyKey(prop)) {
      return "yes but requires restart of the " + prop.getKey().split("[.]")[0];
    }
    return "yes";
  }

  /**
   * Generates documentation for accumulo.properties file usage. Arguments are: "--generate-markdown
   * filename"
   *
   * @param args command-line arguments
   * @throws IllegalArgumentException if args is invalid
   */
  public static void main(String[] args) throws IOException {
    if (args.length == 2 && args[0].equals("--generate-markdown")) {
      try (var printStream = new PrintStream(args[1], UTF_8)) {
        new ConfigurationDocGen(printStream).generate();
      }
    } else {
      throw new IllegalArgumentException(
          "Usage: " + ConfigurationDocGen.class.getName() + " --generate-markdown <filename>");
    }
  }
}
