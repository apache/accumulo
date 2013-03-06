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
package org.apache.accumulo.core.util;

import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.handler.Authenticator;
import org.apache.hadoop.fs.Path;

/**
 * 
 */
public class LoginProperties {
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(LoginProperties.class.getName(), args);
    String indent = "    ";
    
    try {
      String or = null;
      System.out.println("Acceptable login properties are: ");
      Instance instance;
      if (opts.instance == null) {
        @SuppressWarnings("deprecation")
        AccumuloConfiguration deprecatedSiteConfiguration = AccumuloConfiguration.getSiteConfiguration();
        instance = getDefaultInstance(deprecatedSiteConfiguration);
      } else
        instance = opts.getInstance();
      for (Set<Authenticator.AuthProperty> set : instance.getAuthenticator().getProperties()) {
        if (or == null)
          or = "OR";
        else
          System.out.println(or);
        String and = null;
        for (Authenticator.AuthProperty ap : set) {
          if (and == null)
            and = indent + "AND";
          else
            System.out.println(and);
          System.out.println(indent + ap);
        }
      }
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings("deprecation")
  private static Instance getDefaultInstance(AccumuloConfiguration conf) {
    String keepers = conf.get(Property.INSTANCE_ZK_HOST);
    Path instanceDir = new Path(conf.get(Property.INSTANCE_DFS_DIR), "instance_id");
    return new ZooKeeperInstance(UUID.fromString(ZooKeeperInstance.getInstanceIDFromHdfs(instanceDir)), keepers);
  }
  
}
