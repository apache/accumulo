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
package org.apache.accumulo.core.cli;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientInfoImpl;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.IParameterSplitter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ClientOpts extends Help {

  public static class AuthConverter implements IStringConverter<Authorizations> {
    @Override
    public Authorizations convert(String value) {
      return new Authorizations(value.split(","));
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

  public static class PasswordConverter implements IStringConverter<String> {
    public static final String STDIN = "stdin";

    private enum KeyType {
      PASS("pass:"), ENV("env:") {
        @Override
        String process(String value) {
          return System.getenv(value);
        }
      },
      FILE("file:") {
        @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
            justification = "app is run in same security context as user providing the filename")
        @Override
        String process(String value) {
          try (Scanner scanner = new Scanner(Path.of(value), UTF_8)) {
            return scanner.nextLine();
          } catch (IOException e) {
            throw new ParameterException(e);
          }
        }
      },
      STDIN(PasswordConverter.STDIN) {
        @Override
        public boolean matches(String value) {
          return prefix.equals(value);
        }

        @Override
        public String convert(String value) {
          // Will check for this later
          return prefix;
        }
      };

      String prefix;

      private KeyType(String prefix) {
        this.prefix = prefix;
      }

      public boolean matches(String value) {
        return value.startsWith(prefix);
      }

      public String convert(String value) {
        return process(value.substring(prefix.length()));
      }

      String process(String value) {
        return value;
      }
    }

    @Override
    public String convert(String value) {
      for (KeyType keyType : KeyType.values()) {
        if (keyType.matches(value)) {
          return keyType.convert(value);
        }
      }

      return value;
    }
  }

  public static final String OPT_CONFIG_FILE_SHORT = "-c";
  public static final String OPT_CONFIG_FILE_LONG = "--config-file";

  public static final String LEGACY_OPT_PASSWORD = "-p";
  public static final String LEGACY_OPT_TOKEN_CLASS_SHORT = "-tc";
  public static final String LEGACY_OPT_TOKEN_CLASS_LONG = "--tokenClass";
  public static final String LEGACY_OPT_INSTANCE_SHORT = "-i";
  public static final String LEGACY_OPT_INSTANCE_LONG = "--instance";
  public static final String LEGACY_OPT_SITE_FILE = "--site-file";
  public static final String LEGACY_OPT_KEYTAB = "--keytab";
  public static final String LEGACY_OPT_DEBUG = "--debug";
  public static final String LEGACY_OPT_FAKE = "--fake";
  public static final String LEGACY_OPT_MOCK = "--mock";
  public static final String LEGACY_OPT_SSL = "--ssl";
  public static final String LEGACY_OPT_SASL = "--sasl";

  @Parameter(names = LEGACY_OPT_PASSWORD, hidden = true)
  private String legacyPassword;

  @Parameter(names = {LEGACY_OPT_TOKEN_CLASS_SHORT, LEGACY_OPT_TOKEN_CLASS_LONG}, hidden = true)
  private String legacyTokenClass;

  @Parameter(names = {LEGACY_OPT_INSTANCE_SHORT, LEGACY_OPT_INSTANCE_LONG}, hidden = true)
  private String legacyInstance;

  @Parameter(names = LEGACY_OPT_SITE_FILE, hidden = true)
  private String legacySiteFile;

  @Parameter(names = LEGACY_OPT_KEYTAB, hidden = true)
  private String legacyKeytab;

  @Parameter(names = LEGACY_OPT_DEBUG, hidden = true)
  private boolean legacyDebug;

  @Parameter(names = LEGACY_OPT_FAKE, hidden = true)
  private boolean legacyFake;

  @Parameter(names = LEGACY_OPT_MOCK, hidden = true)
  private boolean legacyMock;

  @Parameter(names = LEGACY_OPT_SSL, hidden = true)
  private boolean legacySsl;

  @Parameter(names = LEGACY_OPT_SASL, hidden = true)
  private boolean legacySasl;

  @Parameter(names = {"-u", "--user"}, description = "Connection user")
  public String principal = null;

  @Parameter(names = "--password", converter = PasswordConverter.class,
      description = "connection password (can be specified as '<password>', 'pass:<password>',"
          + " 'file:<local file containing the password>' or 'env:<variable containing"
          + " the pass>')",
      password = true)
  private String securePassword = null;

  public AuthenticationToken getToken() {
    return ClientProperty.getAuthenticationToken(getClientProps());
  }

  @Parameter(names = {"-auths", "--auths"}, converter = AuthConverter.class,
      description = "the authorizations to use when reading or writing")
  public Authorizations auths = Authorizations.EMPTY;

  @Parameter(names = {OPT_CONFIG_FILE_SHORT, OPT_CONFIG_FILE_LONG},
      description = "Read the given client config file. "
          + "If omitted, the classpath will be searched for file named accumulo-client.properties")
  private String clientConfigFile = null;

  @Parameter(names = "-o", splitter = NullSplitter.class, description = "Overrides property in "
      + "accumulo-client.properties. Expected format: -o <key>=<value>")
  private List<String> overrides = new ArrayList<>();

  public Map<String,String> getOverrides() {
    return ServerOpts.getOverrides(overrides);
  }

  @Override
  public void validateArgs() {
    Set<String> options = getPopulatedLegacyOptions();
    if (!options.isEmpty()) {
      String optionsStr = String.join(" ", options);
      throw new IllegalArgumentException("The Client options " + optionsStr
          + " have been dropped. Use accumulo-client.properties for any connection or"
          + " token options. See '" + ClientOpts.OPT_CONFIG_FILE_SHORT + ", "
          + ClientOpts.OPT_CONFIG_FILE_LONG + "' option.");
    }
  }

  /**
   * Get the list of legacy options provided to the Accumulo client.
   *
   * @return the legacy options
   */
  private Set<String> getPopulatedLegacyOptions() {
    Set<String> options = new LinkedHashSet<>();
    if (legacyPassword != null) {
      options.add(LEGACY_OPT_PASSWORD);
    }
    if (legacyTokenClass != null) {
      // Add both the short and long form.
      options.add(LEGACY_OPT_TOKEN_CLASS_SHORT);
      options.add(LEGACY_OPT_TOKEN_CLASS_LONG);
    }
    if (legacyInstance != null) {
      // Add both the short and long form.
      options.add(LEGACY_OPT_INSTANCE_SHORT);
      options.add(LEGACY_OPT_INSTANCE_LONG);
    }
    if (legacySiteFile != null) {
      options.add(LEGACY_OPT_SITE_FILE);
    }
    if (legacyKeytab != null) {
      options.add(LEGACY_OPT_KEYTAB);
    }
    if (legacyDebug) {
      options.add(LEGACY_OPT_DEBUG);
    }
    if (legacyFake) {
      options.add(LEGACY_OPT_FAKE);
    }
    if (legacyMock) {
      options.add(LEGACY_OPT_MOCK);
    }
    if (legacySsl) {
      options.add(LEGACY_OPT_SSL);
    }
    if (legacySasl) {
      options.add(LEGACY_OPT_SASL);
    }
    return options;
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
