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
package org.apache.accumulo.monitor.servlets.trace;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.Properties;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.servlets.BasicServlet;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.tracer.TraceFormatter;
import org.apache.hadoop.security.UserGroupInformation;

abstract class Basic extends BasicServlet {

  private static final long serialVersionUID = 1L;

  public static String getStringParameter(HttpServletRequest req, String name, String defaultValue) {
    String result = req.getParameter(name);
    if (result == null) {
      return defaultValue;
    }
    return result;
  }

  public static int getIntParameter(HttpServletRequest req, String name, int defaultMinutes) {
    String valueString = req.getParameter(name);
    if (valueString == null)
      return defaultMinutes;
    int result = 0;
    try {
      result = Integer.parseInt(valueString);
    } catch (NumberFormatException ex) {
      return defaultMinutes;
    }
    return result;
  }

  public static String dateString(long millis) {
    return TraceFormatter.formatDate(new Date(millis));
  }

  protected Entry<Scanner,UserGroupInformation> getScanner(final StringBuilder sb) throws AccumuloException, AccumuloSecurityException {
    AccumuloConfiguration conf = Monitor.getContext().getConfiguration();
    final boolean saslEnabled = conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED);
    UserGroupInformation traceUgi = null;
    final String principal;
    final AuthenticationToken at;
    Map<String,String> loginMap = conf.getAllPropertiesWithPrefix(Property.TRACE_TOKEN_PROPERTY_PREFIX);
    // May be null
    String keytab = loginMap.get(Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey() + "keytab");

    if (saslEnabled && null != keytab) {
      principal = SecurityUtil.getServerPrincipal(conf.get(Property.TRACE_USER));
      try {
        traceUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
      } catch (IOException e) {
        throw new RuntimeException("Failed to login as trace user", e);
      }
    } else {
      principal = conf.get(Property.TRACE_USER);
    }

    if (!saslEnabled) {
      if (loginMap.isEmpty()) {
        Property p = Property.TRACE_PASSWORD;
        at = new PasswordToken(conf.get(p).getBytes(UTF_8));
      } else {
        Properties props = new Properties();
        int prefixLength = Property.TRACE_TOKEN_PROPERTY_PREFIX.getKey().length();
        for (Entry<String,String> entry : loginMap.entrySet()) {
          props.put(entry.getKey().substring(prefixLength), entry.getValue());
        }

        AuthenticationToken token = Property.createInstanceFromPropertyName(conf, Property.TRACE_TOKEN_TYPE, AuthenticationToken.class, new PasswordToken());
        token.init(props);
        at = token;
      }
    } else {
      at = null;
    }

    final String table = conf.get(Property.TRACE_TABLE);
    Scanner scanner;
    if (null != traceUgi) {
      try {
        scanner = traceUgi.doAs(new PrivilegedExceptionAction<Scanner>() {

          @Override
          public Scanner run() throws Exception {
            // Make the KerberosToken inside the doAs
            AuthenticationToken token = at;
            if (null == token) {
              token = new KerberosToken();
            }
            return getScanner(table, principal, token, sb);
          }

        });
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Failed to obtain scanner", e);
      }
    } else {
      if (null == at) {
        throw new AssertionError("AuthenticationToken should not be null");
      }
      scanner = getScanner(table, principal, at, sb);
    }

    return new AbstractMap.SimpleEntry<Scanner,UserGroupInformation>(scanner, traceUgi);
  }

  private Scanner getScanner(String table, String principal, AuthenticationToken at, StringBuilder sb) throws AccumuloException, AccumuloSecurityException {
    try {
      Connector conn = HdfsZooInstance.getInstance().getConnector(principal, at);
      if (!conn.tableOperations().exists(table)) {
        return new NullScanner();
      }
      Scanner scanner = conn.createScanner(table, conn.securityOperations().getUserAuthorizations(principal));
      return scanner;
    } catch (AccumuloSecurityException ex) {
      sb.append("<h2>Unable to read trace table: check trace username and password configuration.</h2>\n");
      return null;
    } catch (TableNotFoundException ex) {
      return new NullScanner();
    }
  }
}
