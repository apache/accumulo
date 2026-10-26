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
package org.apache.accumulo.core.spi.wal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * Storage provider for TabletServer write-ahead logs. Implementations are initialized once per
 * server process and must be thread-safe because Accumulo may create, read, and delete different
 * logs concurrently. The provider stores an opaque byte stream: Accumulo writes the log header and
 * records to the returned output stream, and a reader must return those exact bytes in their
 * original write order. Configured implementations must have a public no-argument constructor.
 *
 * <p>
 * A provider ID is persisted with every log reference. It must match {@code [A-Za-z0-9]+} and
 * remain stable for as long as any log created by the provider can be referenced. Every provider
 * that may be needed by existing tablet metadata or WAL markers must be configured as available.
 *
 * <p>
 * To keep Accumulo's existing recovery pipeline, log references must be fully qualified Hadoop
 * paths in the standard WAL layout, ending in {@code <host+port>/<uuid>}. Providers may use a
 * custom Hadoop filesystem URI, but must preserve this path layout.
 *
 * @since 4.0.0
 */
public interface WalProvider {

  /** Regular expression for valid provider IDs. */
  String PROVIDER_ID_REGEX = "[A-Za-z0-9]+";

  /**
   * Returns the stable identifier persisted with logs created by this provider. It must be unique
   * among the configured providers and match {@link #PROVIDER_ID_REGEX}.
   *
   * @return stable provider ID
   */
  String getId();

  /**
   * Initializes this provider with its configured options. Implementations may retain the service
   * environment for accessing system configuration and must not assume that properties change
   * during this process lifetime. Throwing an exception prevents the server process from starting.
   *
   * @param params the service environment and options configured for this provider
   * @since 4.0.0
   */
  default void init(InitParameters params) {}

  /** Parameters passed to {@link #init(InitParameters)}. */
  interface InitParameters {

    /** @return system configuration and Accumulo service information */
    ServiceEnvironment getServiceEnvironment();

    /** @return the provider-specific options configured in {@code instance.wal.providers} */
    Map<String,String> getOptions();
  }

  /**
   * Creates a new exclusive WAL for the given TabletServer and UUID. The returned reference must be
   * unique, fully qualified, and end in the supplied server address (with {@code :} replaced by
   * {@code +}) followed by the supplied UUID. The writer must preserve every byte written to its
   * output stream in order.
   *
   * @param serverAddress advertised TabletServer address in {@code host:port} form
   * @param id globally unique identifier for the WAL
   * @return a new writer whose reference can later be passed to {@link #open(String)} and
   *         {@link #delete(String)}
   */
  Writer create(String serverAddress, UUID id) throws IOException;

  /**
   * Opens a completed or abandoned WAL for recovery. The returned stream must start at byte zero
   * and reproduce the bytes written by the corresponding writer exactly and in the same order.
   * Accumulo only opens logs after their TabletServer has closed or lost ownership of them.
   *
   * @param reference reference returned by {@link Writer#getReference()}
   */
  InputStream open(String reference) throws IOException;

  /**
   * Checks whether a WAL reference currently exists without opening it. This may be called while a
   * WAL is still being written.
   *
   * @param reference reference returned by {@link Writer#getReference()}
   * @return true if the WAL exists
   */
  boolean exists(String reference) throws IOException;

  /**
   * Deletes a WAL that Accumulo has determined is no longer referenced by any tablet. Deletion
   * should be idempotent because a server may retry cleanup after an interruption.
   *
   * @param reference reference returned by {@link Writer#getReference()}
   */
  void delete(String reference) throws IOException;

  /**
   * A newly created WAL and its append-only output stream. Accumulo may call
   * {@link #sync(Durability)} while other threads append through the output stream; implementations
   * must ensure a sync covers all bytes written before it is requested.
   */
  interface Writer extends AutoCloseable {

    /**
     * Returns the stable, fully qualified reference for this WAL. It is persisted in tablet
     * metadata and WAL markers and must remain usable by {@link WalProvider#open(String)} and
     * {@link WalProvider#delete(String)}.
     */
    String getReference();

    /**
     * Returns the stream Accumulo writes the WAL header and records to. Accumulo owns and closes
     * this stream through {@link #close()}; implementations may buffer internally but must preserve
     * every byte and its order, and make buffered bytes readable after the corresponding sync or
     * close completes.
     */
    OutputStream getOutputStream();

    /**
     * Makes all preceding bytes durable according to Accumulo's requested durability. {@code FLUSH}
     * must survive a TabletServer process failure and {@code SYNC} must satisfy the filesystem
     * synchronization guarantee used by Accumulo for synced WAL writes. This method is not called
     * for {@code LOG} durability.
     */
    void sync(Durability durability) throws IOException;

    /**
     * Returns the number of bytes written so far, including the WAL header, for TabletServer WAL
     * metrics and diagnostics. This method may run concurrently with writes.
     */
    long getLength() throws IOException;

    /**
     * Returns the current replication level when the provider can report one, or {@code -1} when
     * that information is unavailable.
     */
    default int getCurrentReplication() throws IOException {
      return -1;
    }

    /**
     * Returns a diagnostic description of the current storage pipeline, or an empty array when the
     * provider has no pipeline information.
     */
    default String[] getPipeline() {
      return new String[0];
    }

    /**
     * Closes the WAL and releases all associated resources. Closing must be idempotent because
     * Accumulo may close a writer after an initialization error.
     */
    @Override
    void close() throws IOException;
  }
}
