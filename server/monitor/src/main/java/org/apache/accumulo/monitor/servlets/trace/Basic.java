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

import static com.google.common.base.Charsets.UTF_8;

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
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.trace.TraceFormatter;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.servlets.BasicServlet;
import org.apache.accumulo.server.client.HdfsZooInstance;

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

  protected Scanner getScanner(StringBuilder sb) throws AccumuloException, AccumuloSecurityException {
    AccumuloConfiguration conf = Monitor.getSystemConfiguration();
    String principal = conf.get(Property.TRACE_USER);
    AuthenticationToken at;
    Map<String,String> loginMap = conf.getAllPropertiesWithPrefix(Property.TRACE_TOKEN_PROPERTY_PREFIX);
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

    String table = conf.get(Property.TRACE_TABLE);
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
