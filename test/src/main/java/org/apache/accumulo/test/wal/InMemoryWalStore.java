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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An in-memory WAL store shared by {@link InMemoryWalProvider} instances in separate
 * MiniAccumuloCluster processes. It listens on an available loopback TCP port, keeps WAL contents
 * in the test process heap, and provides the host and port options needed to connect providers.
 * Start the store before starting the cluster and keep it running until the cluster has stopped.
 * Bytes appended by a provider are retained here after its sync or close, so they remain available
 * to other provider processes if a TabletServer exits.
 */
public final class InMemoryWalStore implements AutoCloseable {

  static final byte CREATE = 1;
  static final byte APPEND = 2;
  static final byte READ = 3;
  static final byte EXISTS = 4;
  static final byte DELETE = 5;
  private static final int CONNECT_TIMEOUT_MILLIS = 10_000;

  private final ServerSocket serverSocket;
  private final ExecutorService handlers = Executors.newCachedThreadPool(r -> {
    Thread thread = new Thread(r, "in-memory-wal-handler");
    thread.setDaemon(true);
    return thread;
  });
  private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
  private final Map<String,WalData> wals = new ConcurrentHashMap<>();
  private final Thread acceptThread;
  private volatile boolean closed;

  private InMemoryWalStore(ServerSocket serverSocket) {
    this.serverSocket = serverSocket;
    acceptThread = new Thread(this::acceptConnections, "in-memory-wal-acceptor");
    acceptThread.setDaemon(true);
    acceptThread.start();
  }

  /** Starts a store listening on an available loopback port. */
  @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET",
      justification = "This class is only used in tests.")
  public static InMemoryWalStore start() throws IOException {
    ServerSocket serverSocket = new ServerSocket();
    serverSocket.setReuseAddress(true);
    serverSocket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
    return new InMemoryWalStore(serverSocket);
  }

  /** Returns provider options that connect to this store. */
  public Map<String,String> getProviderOptions() {
    return Map.of(InMemoryWalProvider.HOST_OPTION, "127.0.0.1", InMemoryWalProvider.PORT_OPTION,
        Integer.toString(serverSocket.getLocalPort()));
  }

  private void acceptConnections() {
    while (!closed) {
      try {
        Socket socket = serverSocket.accept();
        connections.add(socket);
        handlers.execute(() -> handle(socket));
      } catch (IOException e) {
        if (!closed) {
          close();
        }
      }
    }
  }

  private void handle(Socket socket) {
    try (socket; var input = new DataInputStream(socket.getInputStream());
        var output = new DataOutputStream(socket.getOutputStream())) {
      byte command = input.readByte();
      String reference = input.readUTF();
      try {
        switch (command) {
          case CREATE -> {
            if (wals.putIfAbsent(reference, new WalData()) != null) {
              throw new IOException("WAL already exists: " + reference);
            }
            output.writeBoolean(true);
          }
          case APPEND -> {
            int length = input.readInt();
            if (length < 0) {
              throw new IOException("Invalid append length: " + length);
            }
            byte[] bytes = input.readNBytes(length);
            if (bytes.length != length) {
              throw new EOFException("Incomplete WAL append");
            }
            getWal(reference).append(bytes);
            output.writeBoolean(true);
          }
          case READ -> {
            byte[] bytes = getWal(reference).snapshot();
            output.writeBoolean(true);
            output.writeInt(bytes.length);
            output.write(bytes);
          }
          case EXISTS -> {
            output.writeBoolean(true);
            output.writeBoolean(wals.containsKey(reference));
          }
          case DELETE -> {
            wals.remove(reference);
            output.writeBoolean(true);
          }
          default -> throw new IOException("Unknown in-memory WAL operation: " + command);
        }
        output.flush();
      } catch (IOException | RuntimeException e) {
        output.writeBoolean(false);
        output.writeUTF(e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        output.flush();
      }
    } catch (IOException e) {
      // The client may have disconnected before the response was sent.
    } finally {
      connections.remove(socket);
    }
  }

  private WalData getWal(String reference) throws IOException {
    WalData wal = wals.get(reference);
    if (wal == null) {
      throw new IOException("WAL does not exist: " + reference);
    }
    return wal;
  }

  private static final class WalData {
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    synchronized void append(byte[] append) {
      bytes.writeBytes(append);
    }

    synchronized byte[] snapshot() {
      return bytes.toByteArray();
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SOCKET",
      justification = "This class is only used in tests.")
  static Socket connect(String host, int port) throws IOException {
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
    return socket;
  }

  static void sendRequest(DataOutputStream output, byte command, String reference)
      throws IOException {
    output.writeByte(command);
    output.writeUTF(reference);
  }

  static void checkResponse(DataInputStream input) throws IOException {
    if (!input.readBoolean()) {
      throw new IOException(input.readUTF());
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      try {
        serverSocket.close();
      } catch (IOException e) {
        // Closing the listener is best effort during teardown.
      }
      for (Socket socket : connections) {
        try {
          socket.close();
        } catch (IOException e) {
          // Closing active requests is best effort during teardown.
        }
      }
      handlers.shutdownNow();
      if (Thread.currentThread() != acceptThread) {
        try {
          acceptThread.join(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
