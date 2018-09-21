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
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;

/**
 * Goal which starts an instance of {@link MiniAccumuloCluster}.
 */
@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST,
    requiresDependencyResolution = ResolutionScope.TEST)
public class StartMojo extends AbstractAccumuloMojo {

  @Parameter(defaultValue = "${project.build.directory}", alias = "outputDirectory",
      property = "accumulo.outputDirectory", required = true)
  private File outputDirectory;

  @Parameter(defaultValue = "testInstance", alias = "instanceName",
      property = "accumulo.instanceName", required = true)
  private String instanceName;

  @Parameter(defaultValue = "secret", alias = "rootPassword", property = "accumulo.rootPassword",
      required = true)
  private String rootPassword;

  @Parameter(defaultValue = "0", alias = "zooKeeperPort", property = "accumulo.zooKeeperPort",
      required = true)
  private int zooKeeperPort;

  private String miniClasspath;

  static Set<MiniAccumuloClusterImpl> runningClusters = Collections
      .synchronizedSet(new HashSet<MiniAccumuloClusterImpl>());

  @Override
  public void execute() throws MojoExecutionException {
    if (shouldSkip()) {
      return;
    }

    File subdir = new File(new File(outputDirectory, "accumulo-maven-plugin"), instanceName);

    try {
      subdir = subdir.getCanonicalFile();
      if (subdir.exists())
        FileUtils.forceDelete(subdir);
      if (!subdir.mkdirs() && !subdir.isDirectory())
        throw new IOException(subdir + " cannot be created as a directory");
      MiniAccumuloConfigImpl cfg = new MiniAccumuloConfigImpl(subdir, rootPassword);
      cfg.setInstanceName(instanceName);
      cfg.setZooKeeperPort(zooKeeperPort);
      configureMiniClasspath(cfg, miniClasspath);
      MiniAccumuloClusterImpl mac = new MiniAccumuloClusterImpl(cfg);
      getLog().info("Starting MiniAccumuloCluster: " + mac.getInstanceName() + " in "
          + mac.getConfig().getDir());
      mac.start();
      runningClusters.add(mac);
    } catch (Exception e) {
      throw new MojoExecutionException(
          "Unable to start " + MiniAccumuloCluster.class.getSimpleName(), e);
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
