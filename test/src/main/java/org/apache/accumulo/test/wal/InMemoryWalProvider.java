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
package org.apache.accumulo.test.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.spi.wal.WalProvider;

/**
 * In-memory WAL provider intended for integration tests using MiniAccumuloCluster. WAL bytes are
 * held by an {@link InMemoryWalStore} running in the test process and accessed over loopback TCP,
 * allowing the cluster's separate server processes to share them. Start the store before starting
 * the cluster and pass {@link InMemoryWalStore#getProviderOptions()} as this provider's options.
 * References use a local filesystem URI with the standard WAL path layout so Accumulo can place
 * sorted recovery logs in the corresponding recovery directory; the WAL bytes themselves remain in
 * the in-memory store. Writes are buffered in the provider process and sent to the store on
 * {@code sync} or {@code close}; therefore synced bytes remain available for recovery after a
 * TabletServer process exits, while unsynced bytes may be lost with that process.
 */
public class InMemoryWalProvider implements WalProvider {

  public static final String PROVIDER_ID = "mem";
  public static final String HOST_OPTION = "host";
  public static final String PORT_OPTION = "port";
  private static final String SCHEME = "file";

  private String host;
  private int port = -1;

  @Override
  public String getId() {
    return PROVIDER_ID;
  }

  @Override
  public void init(InitParameters params) {
    Map<String,String> options = params.getOptions();
    host = options.get(HOST_OPTION);
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException(
          "Missing in-memory WAL store option '" + HOST_OPTION + "'");
    }
    String portOption = options.get(PORT_OPTION);
    if (portOption == null) {
      throw new IllegalArgumentException(
          "Missing in-memory WAL store option '" + PORT_OPTION + "'");
    }
    try {
      port = Integer.parseInt(portOption);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid in-memory WAL store port: " + portOption, e);
    }
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("Invalid in-memory WAL store port: " + port);
    }
  }

  @Override
  public Writer create(String serverAddress, UUID id) throws IOException {
    requireInitialized();
    String hostForPath = Objects.requireNonNull(serverAddress).replace(':', '+');
    String reference;
    try {
      reference =
          new URI(SCHEME, null, referenceRoot() + hostForPath + "/" + id, null, null).toString();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid TabletServer address for WAL reference: " + serverAddress, e);
    }
    validateReference(reference);
    try (var socket = InMemoryWalStore.connect(host, port);
        var output = new DataOutputStream(socket.getOutputStream());
        var input = new DataInputStream(socket.getInputStream())) {
      InMemoryWalStore.sendRequest(output, InMemoryWalStore.CREATE, reference);
      output.flush();
      InMemoryWalStore.checkResponse(input);
    }
    return new MemoryWriter(reference);
  }

  @Override
  public InputStream open(String reference) throws IOException {
    requireInitialized();
    validateReference(reference);
    try (var socket = InMemoryWalStore.connect(host, port);
        var output = new DataOutputStream(socket.getOutputStream());
        var input = new DataInputStream(socket.getInputStream())) {
      InMemoryWalStore.sendRequest(output, InMemoryWalStore.READ, reference);
      output.flush();
      InMemoryWalStore.checkResponse(input);
      int length = input.readInt();
      if (length < 0) {
        throw new IOException("Invalid in-memory WAL length: " + length);
      }
      byte[] bytes = input.readNBytes(length);
      if (bytes.length != length) {
        throw new IOException("Incomplete WAL contents for " + reference);
      }
      return new ByteArrayInputStream(bytes);
    }
  }

  @Override
  public boolean exists(String reference) throws IOException {
    requireInitialized();
    validateReference(reference);
    try (var socket = InMemoryWalStore.connect(host, port);
        var output = new DataOutputStream(socket.getOutputStream());
        var input = new DataInputStream(socket.getInputStream())) {
      InMemoryWalStore.sendRequest(output, InMemoryWalStore.EXISTS, reference);
      output.flush();
      InMemoryWalStore.checkResponse(input);
      return input.readBoolean();
    }
  }

  @Override
  public void delete(String reference) throws IOException {
    requireInitialized();
    validateReference(reference);
    try (var socket = InMemoryWalStore.connect(host, port);
        var output = new DataOutputStream(socket.getOutputStream());
        var input = new DataInputStream(socket.getInputStream())) {
      InMemoryWalStore.sendRequest(output, InMemoryWalStore.DELETE, reference);
      output.flush();
      InMemoryWalStore.checkResponse(input);
    }
  }

  private void requireInitialized() throws IOException {
    if (host == null || port < 1) {
      throw new IOException("InMemoryWalProvider has not been initialized");
    }
  }

  private void validateReference(String reference) throws IOException {
    try {
      URI uri = new URI(reference);
      if (!SCHEME.equals(uri.getScheme()) || uri.getAuthority() != null || uri.getQuery() != null
          || uri.getFragment() != null) {
        throw new IOException("Invalid in-memory WAL reference: " + reference);
      }
      String path = uri.getPath();
      if (path == null || !path.startsWith(referenceRoot())) {
        throw new IOException("Invalid in-memory WAL reference: " + reference);
      }
      String[] parts = path.substring(referenceRoot().length()).split("/", -1);
      if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
        throw new IOException("Invalid in-memory WAL reference: " + reference);
      }
      UUID.fromString(parts[1]);
    } catch (URISyntaxException | IllegalArgumentException e) {
      throw new IOException("Invalid in-memory WAL reference: " + reference, e);
    }
  }

  private String referenceRoot() {
    return "/tmp/accumulo-inmemory-wal-" + port + "/wal/";
  }

  private final class MemoryWriter implements Writer {
    private final String reference;
    private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
    private final OutputStream output = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        append(new byte[] {(byte) b}, 0, 1);
      }

      @Override
      public void write(byte[] bytes, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        append(bytes, offset, length);
      }

      @Override
      public void close() throws IOException {
        MemoryWriter.this.close();
      }
    };
    private long length;
    private boolean closed;

    private MemoryWriter(String reference) {
      this.reference = reference;
    }

    private synchronized void append(byte[] bytes, int offset, int count) throws IOException {
      ensureOpen();
      if (count > 0) {
        pending.write(bytes, offset, count);
        length += count;
      }
    }

    private synchronized void flushPending() throws IOException {
      if (pending.size() > 0) {
        byte[] bytes = pending.toByteArray();
        try (var socket = InMemoryWalStore.connect(host, port);
            var output = new DataOutputStream(socket.getOutputStream());
            var input = new DataInputStream(socket.getInputStream())) {
          InMemoryWalStore.sendRequest(output, InMemoryWalStore.APPEND, reference);
          output.writeInt(bytes.length);
          output.write(bytes);
          output.flush();
          InMemoryWalStore.checkResponse(input);
        }
        pending.reset();
      }
    }

    private void ensureOpen() throws IOException {
      if (closed) {
        throw new IOException("WAL writer is closed: " + reference);
      }
    }

    @Override
    public String getReference() {
      return reference;
    }

    @Override
    public OutputStream getOutputStream() {
      return output;
    }

    @Override
    public synchronized void sync(Durability durability) throws IOException {
      ensureOpen();
      if (durability != Durability.SYNC && durability != Durability.FLUSH) {
        throw new IllegalArgumentException("Unexpected WAL sync durability " + durability);
      }
      flushPending();
    }

    @Override
    public synchronized long getLength() throws IOException {
      return length;
    }

    @Override
    public synchronized void close() throws IOException {
      if (!closed) {
        flushPending();
        closed = true;
      }
    }
  }
}
