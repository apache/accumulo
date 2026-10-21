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

import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_DEBUG;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_FAKE;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_INSTANCE_LONG;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_INSTANCE_SHORT;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_KEYTAB;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_MOCK;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_PASSWORD;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_SASL;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_SITE_FILE;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_SSL;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_TOKEN_CLASS_LONG;
import static org.apache.accumulo.core.cli.ClientOpts.LEGACY_OPT_TOKEN_CLASS_SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Properties;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestClientOpts {

  /**
   * Provide a stream of arguments with the following parameters:
   * <ol>
   * <li>The option</li>
   * <li>Whether the option is a boolean flag</li>
   * <li>What is expected to be present in the error message</li>
   * </ol>
   *
   * @return the arguments
   */
  private static Stream<Arguments> provideLegacyOptions() {
    // @formatter:off
    return Stream.of(
                    arguments(LEGACY_OPT_PASSWORD, false, LEGACY_OPT_PASSWORD),
                    arguments(LEGACY_OPT_TOKEN_CLASS_SHORT, false, LEGACY_OPT_TOKEN_CLASS_SHORT + " " + LEGACY_OPT_TOKEN_CLASS_LONG),
                    arguments(LEGACY_OPT_TOKEN_CLASS_LONG, false, LEGACY_OPT_TOKEN_CLASS_SHORT + " " + LEGACY_OPT_TOKEN_CLASS_LONG),
                    arguments(LEGACY_OPT_INSTANCE_SHORT, false, LEGACY_OPT_INSTANCE_SHORT + " " + LEGACY_OPT_INSTANCE_LONG),
                    arguments(LEGACY_OPT_INSTANCE_LONG, false, LEGACY_OPT_INSTANCE_SHORT + " " + LEGACY_OPT_INSTANCE_LONG),
                    arguments(LEGACY_OPT_SITE_FILE, false, LEGACY_OPT_SITE_FILE),
                    arguments(LEGACY_OPT_KEYTAB, false, LEGACY_OPT_KEYTAB),
                    arguments(LEGACY_OPT_DEBUG, true, LEGACY_OPT_DEBUG),
                    arguments(LEGACY_OPT_FAKE, true, LEGACY_OPT_FAKE),
                    arguments(LEGACY_OPT_MOCK, true, LEGACY_OPT_MOCK),
                    arguments(LEGACY_OPT_SSL, true, LEGACY_OPT_SSL),
                    arguments(LEGACY_OPT_SASL, true, LEGACY_OPT_SASL)
    );
    // @formatter:on
  }

  /**
   * Verify that if the given legacy option is provided to the Accumulo client, an error is thrown.
   *
   * @param option the option name
   * @param isFlag whether the option is a flag
   * @param formattedOption what we expect to see in the error message
   */
  @DisplayName("Verify legacy options result in exception")
  @ParameterizedTest(name = "Option: {0}, Flag: {1}, Expected in message: ''{2}''")
  @Order(1)
  @MethodSource("provideLegacyOptions")
  void testLegacyOptions(String option, boolean isFlag, String formattedOption) {
    String[] args = isFlag ? new String[] {option} : new String[] {option, "value"};
    ClientOpts opts = new ClientOpts();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> opts.parseArgs("test", args));
    assertTrue(exception.getMessage()
        .contains("The Client options " + formattedOption + " have been dropped."));
  }

  /**
   * Verify that if multiple legacy options are provided to the Accumulo client, they are all
   * included in the error message.
   */
  @Test
  void testMultipleLegacyOptionsAreAllListedInException() {
    String[] args =
        {LEGACY_OPT_PASSWORD, "1234", LEGACY_OPT_INSTANCE_SHORT, "myInstance", LEGACY_OPT_DEBUG};
    ClientOpts opts = new ClientOpts();
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> opts.parseArgs("test", args));
    assertTrue(exception.getMessage()
        .contains("The Client options -p -i --instance --debug have been dropped."));
  }

  @Test
  public void testBasic() {
    ClientOpts opts = new ClientOpts();
    String[] args = new String[] {"-u", "userabc", "-o", "instance.name=myinst", "-o",
        "instance.zookeepers=zoo1,zoo2", "-o", "auth.type=password", "-o", "auth.principal=user123",
        "-o", "auth.token=mypass"};
    opts.parseArgs("test", args);
    Properties props = opts.getClientProps();
    assertEquals("user123", ClientProperty.AUTH_PRINCIPAL.getValue(props));
    assertTrue(opts.getToken() instanceof PasswordToken);
    assertEquals("myinst", props.getProperty("instance.name"));

    opts = new ClientOpts();
    args = new String[] {"-o", "instance.name=myinst", "-o", "instance.zookeepers=zoo1,zoo2", "-o",
        "auth.type=password", "-o", "auth.token=mypass", "-u", "userabc"};
    opts.parseArgs("test", args);
    props = opts.getClientProps();
    assertEquals("userabc", ClientProperty.AUTH_PRINCIPAL.getValue(props));
    assertTrue(opts.getToken() instanceof PasswordToken);
    assertEquals("myinst", props.getProperty("instance.name"));
  }

  @Test
  public void testPassword() {
    ClientOpts opts = new ClientOpts();
    String[] args =
        new String[] {"--password", "mypass", "-u", "userabc", "-o", "instance.name=myinst", "-o",
            "instance.zookeepers=zoo1,zoo2", "-o", "auth.principal=user123"};
    opts.parseArgs("test", args);
    Properties props = opts.getClientProps();
    assertEquals("user123", ClientProperty.AUTH_PRINCIPAL.getValue(props));
    assertTrue(opts.getToken() instanceof PasswordToken);
    assertTrue(opts.getToken().equals(new PasswordToken("mypass")));
    assertEquals("myinst", props.getProperty("instance.name"));
  }

}
