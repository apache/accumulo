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
package org.apache.accumulo.server.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.TokenProperty;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class LoginProperties implements KeywordExecutable {

  @Override
  public String keyword() {
    return "login-info";
  }

  @Override
  public String description() {
    return "Prints Accumulo login info";
  }

  @Override
  public void execute(String[] args) throws Exception {
    AccumuloConfiguration config = new ServerConfigurationFactory(HdfsZooInstance.getInstance()).getSystemConfiguration();
    Authenticator authenticator = AccumuloVFSClassLoader.getClassLoader().loadClass(config.get(Property.INSTANCE_SECURITY_AUTHENTICATOR))
        .asSubclass(Authenticator.class).newInstance();

    List<Set<TokenProperty>> tokenProps = new ArrayList<>();

    for (Class<? extends AuthenticationToken> tokenType : authenticator.getSupportedTokenTypes()) {
      tokenProps.add(tokenType.newInstance().getProperties());
    }

    System.out.println("Supported token types for " + authenticator.getClass().getName() + " are : ");
    for (Class<? extends AuthenticationToken> tokenType : authenticator.getSupportedTokenTypes()) {
      System.out.println("\t" + tokenType.getName() + ", which accepts the following properties : ");

      for (TokenProperty tokenProperty : tokenType.newInstance().getProperties()) {
        System.out.println("\t\t" + tokenProperty);
      }

      System.out.println();
    }
  }

  public static void main(String[] args) throws Exception {
    new LoginProperties().execute(args);
  }
}
