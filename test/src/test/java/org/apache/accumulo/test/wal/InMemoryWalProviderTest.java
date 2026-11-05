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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.wal.WalProvider;
import org.junit.jupiter.api.Test;

class InMemoryWalProviderTest {

  @Test
  void sharesSyncedAndClosedWalBytesAcrossProviders() throws Exception {
    try (var store = InMemoryWalStore.start()) {
      WalProvider writerProvider = newProvider(store.getProviderOptions());
      WalProvider readerProvider = newProvider(store.getProviderOptions());
      UUID id = UUID.randomUUID();
      WalProvider.Writer writer = writerProvider.create("tserver:1234", id);
      String reference = writer.getReference();

      assertEquals("file:/tmp/accumulo-inmemory-wal-"
          + store.getProviderOptions().get(InMemoryWalProvider.PORT_OPTION) + "/wal/tserver+1234/"
          + id, reference);
      assertTrue(readerProvider.exists(reference));

      writer.getOutputStream().write("header-and-synced-data".getBytes(UTF_8));
      assertEquals(22, writer.getLength());
      writer.sync(Durability.FLUSH);
      try (InputStream input = readerProvider.open(reference)) {
        assertArrayEquals("header-and-synced-data".getBytes(UTF_8), input.readAllBytes());
      }

      writer.getOutputStream().write("-tail".getBytes(UTF_8));
      writer.close();
      writer.close();
      try (InputStream input = readerProvider.open(reference)) {
        assertArrayEquals("header-and-synced-data-tail".getBytes(UTF_8), input.readAllBytes());
      }
      assertEquals(27, writer.getLength());

      readerProvider.delete(reference);
      readerProvider.delete(reference);
      assertFalse(writerProvider.exists(reference));
      assertThrows(IOException.class, () -> readerProvider.open(reference));
      assertThrows(IOException.class, () -> writer.getOutputStream().write(1));
    }
  }

  @Test
  void rejectsInvalidProviderOptionsAndReferences() throws Exception {
    InMemoryWalProvider uninitializedProvider = new InMemoryWalProvider();
    assertThrows(IllegalArgumentException.class,
        () -> uninitializedProvider.init(initParameters(Map.of("host", "127.0.0.1"))));

    try (var store = InMemoryWalStore.start()) {
      WalProvider provider = newProvider(store.getProviderOptions());
      assertThrows(IOException.class, () -> provider.exists("file:///tmp/not-a-wal"));
    }
  }

  @Test
  void rejectsLogDurabilitySyncRequests() throws Exception {
    try (var store = InMemoryWalStore.start()) {
      WalProvider provider = newProvider(store.getProviderOptions());
      try (WalProvider.Writer writer = provider.create("tserver:1234", UUID.randomUUID())) {
        assertThrows(IllegalArgumentException.class, () -> writer.sync(Durability.LOG));
      }
    }
  }

  private static WalProvider newProvider(Map<String,String> options) {
    InMemoryWalProvider provider = new InMemoryWalProvider();
    provider.init(initParameters(options));
    return provider;
  }

  private static WalProvider.InitParameters initParameters(Map<String,String> options) {
    return new WalProvider.InitParameters() {
      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return null;
      }

      @Override
      public Map<String,String> getOptions() {
        return options;
      }
    };
  }
}
