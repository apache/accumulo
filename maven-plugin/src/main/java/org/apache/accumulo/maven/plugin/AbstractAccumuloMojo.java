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
package org.apache.accumulo.maven.plugin;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;

public abstract class AbstractAccumuloMojo extends AbstractMojo {
  
  @Parameter(defaultValue = "${plugin.artifacts}", readonly = true, required = true)
  private List<Artifact> pluginArtifacts;
  
  void configureMiniClasspath(String miniClasspath) {
    String classpath = "";
    if (miniClasspath == null && pluginArtifacts != null) {
      String sep = "";
      for (Artifact artifact : pluginArtifacts) {
        try {
          classpath += sep + artifact.getFile().toURI().toURL();
          sep = File.pathSeparator;
        } catch (MalformedURLException e) {
          e.printStackTrace();
        }
      }
    } else if (miniClasspath != null && !miniClasspath.isEmpty()) {
      classpath = miniClasspath;
    }
    System.setProperty("java.class.path", System.getProperty("java.class.path", "") + File.pathSeparator + classpath);
  }
}
