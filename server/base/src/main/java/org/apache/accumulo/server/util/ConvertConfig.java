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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class ConvertConfig implements KeywordExecutable {

  @Override
  public String keyword() {
    return "convert-config";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.OTHER;
  }

  @Override
  public String description() {
    return "Convert Accumulo configuration from XML to properties";
  }

  static class Opts extends Help {

    @Parameter(names = {"-x", "-xml", "--xml"},
        description = "Path of accumulo-site.xml to convert from")
    String xmlPath = "./accumulo-site.xml";

    @Parameter(names = {"-p", "-props", "--props"},
        description = "Path to create new accumulo.properties")
    String propsPath = "./accumulo.properties";
  }

  private static void writeLine(BufferedWriter w, String value) {
    try {
      w.write(value + "\n");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("accumulo convert-config", args);

    File xmlFile = new File(opts.xmlPath);
    if (!xmlFile.exists()) {
      throw new IllegalArgumentException("xml config file does not exist at " + opts.xmlPath);
    }

    Path propsPath = Paths.get(opts.propsPath);
    if (propsPath.toFile().exists()) {
      throw new IllegalArgumentException("properties file already exists at " + opts.propsPath);
    }

    Configuration xmlConfig = new Configuration(false);
    xmlConfig.addResource(xmlFile.toURI().toURL());

    try (BufferedWriter w = Files.newBufferedWriter(propsPath, UTF_8)) {
      StreamSupport.stream(xmlConfig.spliterator(), false).sorted(Map.Entry.comparingByKey())
          .forEach(e -> writeLine(w, e.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    new ConvertConfig().execute(args);
  }
}
