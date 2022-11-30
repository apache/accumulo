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
package org.apache.accumulo.core.util;

import java.io.Console;

import org.apache.accumulo.core.cli.ClientOpts.PasswordConverter;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.TokenProperty;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CreateToken implements KeywordExecutable {

  private Console reader = null;

  private Console getConsoleReader() {

    if (reader == null) {
      reader = System.console();
    }
    return reader;
  }

  static class Opts extends Help {
    @Parameter(names = {"-u", "--user"}, description = "Connection user")
    public String principal = null;

    @Parameter(names = "-p", converter = PasswordConverter.class,
        description = "Connection password")
    public String password = null;

    @Parameter(names = "--password", converter = PasswordConverter.class,
        description = "Enter the connection password", password = true)
    public String securePassword = null;

    @Parameter(names = {"-tc", "--tokenClass"},
        description = "The class of the authentication token")
    public String tokenClassName = PasswordToken.class.getName();
  }

  public static void main(String[] args) {
    new CreateToken().execute(args);
  }

  @Override
  public String keyword() {
    return "create-token";
  }

  @Override
  public String description() {
    return "Creates authentication token";
  }

  @Override
  public void execute(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs("accumulo create-token", args);

    String pass = opts.password;
    if (pass == null && opts.securePassword != null) {
      pass = opts.securePassword;
    }

    try {
      String principal = opts.principal;
      if (principal == null) {
        principal = getConsoleReader().readLine("Username (aka principal): ");
      }

      AuthenticationToken token = Class.forName(opts.tokenClassName)
          .asSubclass(AuthenticationToken.class).getDeclaredConstructor().newInstance();
      Properties props = new Properties();
      for (TokenProperty tp : token.getProperties()) {
        String input;
        if (pass != null && tp.getKey().equals("password")) {
          input = pass;
        } else {
          if (tp.getMask()) {
            input = getConsoleReader().readLine(tp.getDescription() + ": ", '*');
          } else {
            input = getConsoleReader().readLine(tp.getDescription() + ": ");
          }
        }
        props.put(tp.getKey(), input);
        token.init(props);
      }
      System.out.println("auth.type = " + opts.tokenClassName);
      System.out.println("auth.principal = " + principal);
      System.out.println("auth.token = " + ClientProperty.encodeToken(token));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
