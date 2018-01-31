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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.TreeMap;

/**
 * This class generates documentation to inform users of the available configuration properties in a presentable form.
 */
class ClientConfigGenerate {

  private abstract class Format {

    abstract void beginSection(String section);

    abstract void pageHeader();

    abstract void property(ClientProperty prop);

    void generate() {
      pageHeader();

      generateSection("Instance", "instance.");
      generateSection("Authentication", "auth.", "auth.type");
      generateSection("Batch Writer", "batch.writer.");
      generateSection("SSL", "ssl.");
      generateSection("SASL", "sasl.");
      generateSection("Tracing", "trace.");

      doc.close();
    }

    void generateSection(String section, String prefix, String firstProperty) {
      beginSection(section);
      ClientProperty first = sortedProps.get(firstProperty);
      if (first != null) {
        property(sortedProps.get(firstProperty));
      }
      for (ClientProperty prop : sortedProps.values()) {
        if (prop.getKey().startsWith(prefix) && !prop.getKey().equals(firstProperty)) {
          property(prop);
        }
      }
    }

    void generateSection(String section, String prefix) {
      generateSection(section, prefix, "");
    }
  }

  private class Markdown extends Format {

    @Override
    void beginSection(String section) {}

    @Override
    void pageHeader() {
      doc.println("---");
      doc.println("title: Client Properties");
      doc.println("category: development");
      doc.println("order: 9");
      doc.println("---\n");
      doc.println("<!-- WARNING: Do not edit this file. It is a generated file that is copied from Accumulo build (from core/target/generated-docs) -->\n");
      doc.println("Below are properties set in `accumulo-client.properties` that configure Accumulo clients:\n");
      doc.println("| Property | Default value | Description |");
      doc.println("|----------|---------------|-------------|");
    }

    @Override
    void property(ClientProperty prop) {
      Objects.nonNull(prop);
      doc.print("| <a name=\"" + prop.getKey().replace(".", "_") + "\" class=\"prop\"></a> " + prop.getKey() + " | ");
      String defaultValue = sanitize(prop.getDefaultValue()).trim();
      if (defaultValue.length() == 0) {
        defaultValue = "*empty*";
      }
      doc.print(defaultValue + " | ");
      doc.println(sanitize(prop.getDescription() + " |"));
    }

    String sanitize(String str) {
      return str.replace("\n", "<br>");
    }
  }

  private class ConfigFile extends Format {

    @Override
    void beginSection(String section) {
      doc.println("\n## " + section + " properties");
      doc.println("## --------------");
    }

    @Override
    void pageHeader() {
      doc.println("# Licensed to the Apache Software Foundation (ASF) under one or more");
      doc.println("# contributor license agreements.  See the NOTICE file distributed with");
      doc.println("# this work for additional information regarding copyright ownership.");
      doc.println("# The ASF licenses this file to You under the Apache License, Version 2.0");
      doc.println("# (the \"License\"); you may not use this file except in compliance with");
      doc.println("# the License.  You may obtain a copy of the License at");
      doc.println("#");
      doc.println("#     http://www.apache.org/licenses/LICENSE-2.0");
      doc.println("#");
      doc.println("# Unless required by applicable law or agreed to in writing, software");
      doc.println("# distributed under the License is distributed on an \"AS IS\" BASIS,");
      doc.println("# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
      doc.println("# See the License for the specific language governing permissions and");
      doc.println("# limitations under the License.\n");
      doc.println("################################");
      doc.println("## Accumulo client configuration");
      doc.println("################################\n");
      doc.println("## NOTE - All properties that have a default are set with it. Properties that");
      doc.println("## are uncommented must be set by the user.");
    }

    @Override
    void property(ClientProperty prop) {
      doc.println("## " + prop.getDescription());
      if (!prop.isRequired()) {
        doc.print("#");
      }
      doc.println(prop.getKey() + "=" + prop.getDefaultValue() + "\n");
    }
  }

  private PrintStream doc;
  private final TreeMap<String,ClientProperty> sortedProps = new TreeMap<>();

  private ClientConfigGenerate(PrintStream doc) {
    Objects.nonNull(doc);
    this.doc = doc;
    for (ClientProperty prop : ClientProperty.values()) {
      this.sortedProps.put(prop.getKey(), prop);
    }
  }

  private void generateMarkdown() {
    new Markdown().generate();
  }

  private void generateConfigFile() {
    new ConfigFile().generate();
  }

  /**
   * Generates documentation for accumulo-site.xml file usage. Arguments are: "--generate-markdown filename"
   *
   * @param args
   *          command-line arguments
   * @throws IllegalArgumentException
   *           if args is invalid
   */
  public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
    if (args.length == 2) {
      ClientConfigGenerate clientConfigGenerate = new ClientConfigGenerate(new PrintStream(args[1], UTF_8.name()));
      if (args[0].equals("--generate-markdown")) {
        clientConfigGenerate.generateMarkdown();
        return;
      } else if (args[0].equals("--generate-config")) {
        clientConfigGenerate.generateConfigFile();
        return;
      }
    }
    throw new IllegalArgumentException("Usage: " + ClientConfigGenerate.class.getName() + " [--generate-markdown|--generate-config] <filename>");
  }
}
