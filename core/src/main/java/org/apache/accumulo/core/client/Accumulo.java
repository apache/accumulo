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
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.client.impl.AccumuloClientImpl;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.rfile.RFile;

/**
 * This class contains all API entry points created in 2.0.0 or later. The majority of the API is
 * accessible indirectly via methods in this class. Below are a list of APIs entry points that are
 * not accessible from here.
 *
 * <UL>
 * <LI>Hadoop input, output formats and partitioners in
 * {@code org.apache.accumulo.core.client.mapred} and
 * {@code org.apache.accumulo.core.client.mapreduce} packages (excluding {@code impl} sub-packages).
 * <LI>{@code org.apache.accumulo.minicluster.MiniAccumuloCluster} Not linkable by javadoc, because
 * in a separate module.
 * <LI>{@link Lexicoder} and all of its implementations in the same package (excluding the
 * {@code impl} sub-package).
 * <LI>{@link RFile}
 * </UL>
 *
 * @see <a href="http://accumulo.apache.org/">Accumulo Website</a>
 * @see <a href="http://accumulo.apache.org/api">Accumulo Public API</a>
 * @see <a href="http://semver.org/spec/v2.0.0">Semver 2.0</a>
 * @since 2.0.0
 */
// CHECKSTYLE:ON
public final class Accumulo {

  private Accumulo() {}

  /**
   * Fluent entry point for creating an {@link AccumuloClient}. For example:
   *
   * <pre>
   * <code>
   * try (AccumuloClient client = Accumulo.newClient()
   *        .forInstance(instanceName, zookeepers)
   *        .usingPassword(user, password).build())
   * {
   *   // use the client
   * }
   * </code>
   * </pre>
   *
   * @return a builder object for Accumulo clients
   */
  public static AccumuloClient.ClientInfoOptions newClient() {
    return new AccumuloClientImpl.AccumuloClientBuilderImpl();
  }
}
