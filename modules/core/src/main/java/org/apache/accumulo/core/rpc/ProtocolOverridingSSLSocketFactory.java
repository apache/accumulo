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
package org.apache.accumulo.core.rpc;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * JDK6's SSLSocketFactory doesn't seem to properly set the protocols on the Sockets that it creates which causes an SSLv2 client hello message during
 * handshake, even when only TLSv1 is enabled. This only appears to be an issue on the client sockets, not the server sockets.
 *
 * This class wraps the SSLSocketFactory ensuring that the Socket is properly configured.
 * http://www.coderanch.com/t/637177/Security/Disabling-handshake-message-Java
 *
 * This class can be removed when JDK6 support is officially unsupported by Accumulo
 */
class ProtocolOverridingSSLSocketFactory extends SSLSocketFactory {

  private final SSLSocketFactory delegate;
  private final String[] enabledProtocols;

  public ProtocolOverridingSSLSocketFactory(final SSLSocketFactory delegate, final String[] enabledProtocols) {
    requireNonNull(enabledProtocols);
    checkArgument(0 != enabledProtocols.length, "Expected at least one protocol");
    this.delegate = delegate;
    this.enabledProtocols = enabledProtocols;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return delegate.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return delegate.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(final Socket socket, final String host, final int port, final boolean autoClose) throws IOException {
    final Socket underlyingSocket = delegate.createSocket(socket, host, port, autoClose);
    return overrideProtocol(underlyingSocket);
  }

  @Override
  public Socket createSocket(final String host, final int port) throws IOException, UnknownHostException {
    final Socket underlyingSocket = delegate.createSocket(host, port);
    return overrideProtocol(underlyingSocket);
  }

  @Override
  public Socket createSocket(final String host, final int port, final InetAddress localAddress, final int localPort) throws IOException, UnknownHostException {
    final Socket underlyingSocket = delegate.createSocket(host, port, localAddress, localPort);
    return overrideProtocol(underlyingSocket);
  }

  @Override
  public Socket createSocket(final InetAddress host, final int port) throws IOException {
    final Socket underlyingSocket = delegate.createSocket(host, port);
    return overrideProtocol(underlyingSocket);
  }

  @Override
  public Socket createSocket(final InetAddress host, final int port, final InetAddress localAddress, final int localPort) throws IOException {
    final Socket underlyingSocket = delegate.createSocket(host, port, localAddress, localPort);
    return overrideProtocol(underlyingSocket);
  }

  /**
   * Set the {@link javax.net.ssl.SSLSocket#getEnabledProtocols() enabled protocols} to {@link #enabledProtocols} if the <code>socket</code> is a
   * {@link SSLSocket}
   *
   * @param socket
   *          The Socket
   */
  private Socket overrideProtocol(final Socket socket) {
    if (socket instanceof SSLSocket) {
      ((SSLSocket) socket).setEnabledProtocols(enabledProtocols);
    }
    return socket;
  }
}
