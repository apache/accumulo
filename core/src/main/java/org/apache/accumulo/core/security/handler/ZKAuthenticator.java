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
package org.apache.accumulo.core.security.handler;

import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.tokens.PasswordToken;
import org.apache.accumulo.core.security.thrift.tokens.SecurityToken;

/**
 * 
 */
public class ZKAuthenticator implements Authenticator {
  
  @Override
  public SecurityToken login(Properties properties) throws AccumuloSecurityException{
    if (properties.containsKey("password"))
      return new PasswordToken().setPassword(properties.getProperty("password").getBytes(Charset.forName("UTF-8")));
    throw new AccumuloSecurityException(properties.getProperty("user"), SecurityErrorCode.INSUFFICIENT_PROPERTIES);
  }
}
