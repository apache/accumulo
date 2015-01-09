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

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl.LogWriter;
import org.apache.http.annotation.ThreadSafe;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;

/**
 * Goal which stops all instances of {@link MiniAccumuloCluster} started with the start mojo.
 */
@ThreadSafe
@Mojo(name = "stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST, requiresDependencyResolution = ResolutionScope.TEST)
public class StopMojo extends AbstractAccumuloMojo {

  @Override
  public void execute() throws MojoExecutionException {
    for (MiniAccumuloClusterImpl mac : StartMojo.runningClusters) {
      System.out.println("Stopping MiniAccumuloCluster: " + mac.getInstanceName());
      try {
        mac.stop();
        for (LogWriter log : mac.getLogWriters())
          log.flush();
      } catch (Exception e) {
        throw new MojoExecutionException("Unable to start " + MiniAccumuloCluster.class.getSimpleName(), e);
      }
    }
  }
}
