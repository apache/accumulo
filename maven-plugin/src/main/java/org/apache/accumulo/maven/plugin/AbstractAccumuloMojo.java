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

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.project.MavenProject;

public abstract class AbstractAccumuloMojo extends AbstractMojo {
  
  @Component
  private MavenProject project;
  
  void configureMiniClasspath(String miniClasspath) throws MalformedURLException {
    String classpath = "";
    StringBuilder sb = new StringBuilder();
    if (miniClasspath == null && project != null) {
      sb.append(project.getBuild().getOutputDirectory());
      String sep = File.pathSeparator;
      sb.append(sep).append(project.getBuild().getTestOutputDirectory());
      for (Artifact artifact : project.getArtifacts()) {
        addArtifact(sb, sep, artifact);
      }
      classpath = sb.toString();
    } else if (miniClasspath != null && !miniClasspath.isEmpty()) {
      classpath = miniClasspath;
    }
    System.setProperty("java.class.path", System.getProperty("java.class.path", "") + File.pathSeparator + classpath);
  }
  
  private void addArtifact(StringBuilder classpath, String separator, Artifact artifact) throws MalformedURLException {
    classpath.append(separator).append(artifact.getFile().toURI().toURL());
  }
}
