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
package org.apache.accumulo.server.rpc;

import java.io.IOException;

import javax.security.sasl.SaslServer;

import org.apache.accumulo.core.rpc.SaslConnectionParams.SaslMechanism;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor that pulls the SaslServer object out of the transport, and assumes the remote user's UGI before calling through to the original processor.
 *
 * This is used on the server side to set the UGI for each specific call.
 *
 * Lifted from Apache Hive 0.14
 */
public class UGIAssumingProcessor implements TProcessor {
  private static final Logger log = LoggerFactory.getLogger(UGIAssumingProcessor.class);

  public static final ThreadLocal<String> rpcPrincipal = new ThreadLocal<>();
  public static final ThreadLocal<SaslMechanism> rpcMechanism = new ThreadLocal<>();

  private final TProcessor wrapped;
  private final UserGroupInformation loginUser;

  public UGIAssumingProcessor(TProcessor wrapped) {
    this.wrapped = wrapped;
    try {
      this.loginUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      log.error("Failed to obtain login user", e);
      throw new RuntimeException("Failed to obtain login user", e);
    }
  }

  /**
   * The principal of the user who authenticated over SASL.
   */
  public static String rpcPrincipal() {
    return rpcPrincipal.get();
  }

  public static ThreadLocal<String> getRpcPrincipalThreadLocal() {
    return rpcPrincipal;
  }

  public static SaslMechanism rpcMechanism() {
    return rpcMechanism.get();
  }

  @Override
  public boolean process(final TProtocol inProt, final TProtocol outProt) throws TException {
    TTransport trans = inProt.getTransport();
    if (!(trans instanceof TSaslServerTransport)) {
      throw new TException("Unexpected non-SASL transport " + trans.getClass() + ": " + trans);
    }
    TSaslServerTransport saslTrans = (TSaslServerTransport) trans;
    SaslServer saslServer = saslTrans.getSaslServer();
    String authId = saslServer.getAuthorizationID();
    String endUser = authId;

    SaslMechanism mechanism;
    try {
      mechanism = SaslMechanism.get(saslServer.getMechanismName());
    } catch (Exception e) {
      log.error("Failed to process RPC with SASL mechanism {}", saslServer.getMechanismName());
      throw e;
    }

    switch (mechanism) {
      case GSSAPI:
        UserGroupInformation clientUgi = UserGroupInformation.createProxyUser(endUser, loginUser);
        final String remoteUser = clientUgi.getUserName();

        try {
          // Set the principal in the ThreadLocal for access to get authorizations
          rpcPrincipal.set(remoteUser);

          return wrapped.process(inProt, outProt);
        } finally {
          // Unset the principal after we're done using it just to be sure that it's not incorrectly
          // used in the same thread down the line.
          rpcPrincipal.set(null);
        }
      case DIGEST_MD5:
        // The CallbackHandler, after deserializing the TokenIdentifier in the name, has already updated
        // the rpcPrincipal for us. We don't need to do it again here.
        try {
          rpcMechanism.set(mechanism);
          return wrapped.process(inProt, outProt);
        } finally {
          // Unset the mechanism after we're done using it just to be sure that it's not incorrectly
          // used in the same thread down the line.
          rpcMechanism.set(null);
        }
      default:
        throw new IllegalArgumentException("Cannot process SASL mechanism " + mechanism);
    }
  }
}
