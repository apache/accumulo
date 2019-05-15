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
package org.apache.accumulo.core.cli;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.htrace.NullScope;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;

public class ClientOpts extends Help {

  public static class AuthConverter implements IStringConverter<Authorizations> {
    @Override
    public Authorizations convert(String value) {
      return new Authorizations(value.split(","));
    }
  }

  public static class Password {
    public byte[] value;

    public Password(String dfault) {
      value = dfault.getBytes(UTF_8);
    }

    @Override
    public String toString() {
      return new String(value, UTF_8);
    }
  }

  public static class PasswordConverter implements IStringConverter<Password> {
    @Override
    public Password convert(String value) {
      return new Password(value);
    }
  }

  public static class VisibilityConverter implements IStringConverter<ColumnVisibility> {
    @Override
    public ColumnVisibility convert(String value) {
      return new ColumnVisibility(value);
    }
  }

  public static class NullSplitter implements IParameterSplitter {
    @Override
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  @Parameter(names = {"-u", "--user"}, description = "Connection user")
  public String principal = null;

  @Parameter(names = "--password", converter = PasswordConverter.class,
      description = "Enter the connection password", password = true)
  private Password securePassword = null;

  public AuthenticationToken getToken() {
    return ClientProperty.getAuthenticationToken(getClientProps());
  }

  @Parameter(names = {"-auths", "--auths"}, converter = AuthConverter.class,
      description = "the authorizations to use when reading or writing")
  public Authorizations auths = Authorizations.EMPTY;

  @Parameter(names = {"-c", "--config-file"}, description = "Read the given client config file. "
      + "If omitted, the classpath will be searched for file named accumulo-client.properties")
  private String clientConfigFile = null;

  @Parameter(names = "-o", splitter = NullSplitter.class, description = "Overrides property in "
      + "accumulo-client.properties. Expected format: -o <key>=<value>")
  private List<String> overrides = new ArrayList<>();

  @Parameter(names = "--trace", description = "turn on distributed tracing")
  public boolean trace = false;

  public Map<String,String> getOverrides() {
    return ConfigOpts.getOverrides(overrides);
  }

  public TraceScope parseArgsAndTrace(String programName, String[] args, Object... others) {
    parseArgs(programName, args, others);
    return trace ? Trace.startSpan(programName, Sampler.ALWAYS) : NullScope.INSTANCE;
  }

  @Override
  public void parseArgs(String programName, String[] args, Object... others) {
    super.parseArgs(programName, args, others);
  }

  private Properties cachedProps = null;

  public String getClientConfigFile() {
    if (clientConfigFile == null) {
      URL clientPropsUrl =
          ClientOpts.class.getClassLoader().getResource("accumulo-client.properties");
      if (clientPropsUrl != null) {
        clientConfigFile = clientPropsUrl.getFile();
      }
    }
    return clientConfigFile;
  }

  public Properties getClientProps() {
    if (cachedProps == null) {
      cachedProps = new Properties();
      if (getClientConfigFile() != null) {
        cachedProps = ClientInfoImpl.toProperties(getClientConfigFile());
      }
      if (principal != null) {
        cachedProps.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), principal);
      }
      if (securePassword != null) {
        ClientProperty.setPassword(cachedProps, securePassword.toString());
      }
      getOverrides().forEach((k, v) -> cachedProps.put(k, v));
      ClientProperty.validate(cachedProps);
    }
    return cachedProps;
  }
}
