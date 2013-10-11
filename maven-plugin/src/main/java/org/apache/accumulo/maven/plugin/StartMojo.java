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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.http.annotation.ThreadSafe;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.codehaus.plexus.util.FileUtils;

/**
 * Goal which starts an instance of {@link MiniAccumuloCluster}.
 */
@ThreadSafe
@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST, requiresDependencyResolution = ResolutionScope.TEST)
public class StartMojo extends AbstractAccumuloMojo {

  @Parameter(defaultValue = "${project.build.directory}", property = "outputDir", required = true)
  private File outputDirectory;

  @Parameter(defaultValue = "testInstance", property = "instanceName", required = true)
  private String instanceName;

  @Parameter(defaultValue = "secret", property = "rootPassword", required = true)
  private String rootPassword;

  private String miniClasspath;

  static Set<MiniAccumuloCluster> runningClusters = Collections.synchronizedSet(new HashSet<MiniAccumuloCluster>());

  @Override
  public void execute() throws MojoExecutionException {
    File subdir = new File(new File(outputDirectory, "accumulo-maven-plugin"), instanceName);

    try {
      subdir = subdir.getCanonicalFile();
      if (subdir.exists())
        FileUtils.forceDelete(subdir);
      subdir.mkdirs();
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(subdir, rootPassword);
      cfg.setInstanceName(instanceName);
      configureMiniClasspath(cfg, miniClasspath);
      MiniAccumuloCluster mac = new MiniAccumuloCluster(cfg);
      System.out.println("Starting MiniAccumuloCluster: " + mac.getInstanceName() + " in " + mac.getConfig().getDir());
      mac.start();
      runningClusters.add(mac);
    } catch (Exception e) {
      throw new MojoExecutionException("Unable to start " + MiniAccumuloCluster.class.getSimpleName(), e);
    }

  }

  public static void main(String[] args) throws MojoExecutionException {
    int a = 0;
    for (String arg : args) {
      if (a < 2) {
        // skip the first two args
        a++;
        continue;
      }
      StartMojo starter = new StartMojo();
      starter.outputDirectory = new File(args[0]);
      String[] instArgs = arg.split(" ");
      starter.instanceName = instArgs[0];
      starter.rootPassword = instArgs[1];
      starter.miniClasspath = args[1];
      starter.execute();
    }
  }
}
