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
package org.apache.accumulo.core.tabletserver.log;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Base64;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class LogEntryTest {

  private final String validHost = "localhost+9997";
  private final UUID validUUID = UUID.randomUUID();
  private static final String PROVIDER_ID = "example";
  private final String validPath = "viewfs:/a/accumulo/wal/" + validHost + "/" + validUUID;

  @Test
  public void testColumnFamily() {
    assertEquals(new Text("log"), LogColumnFamily.NAME);
  }

  @Test
  public void testFromPath() {
    var logEntry = LogEntry.fromPath(PROVIDER_ID, validPath);
    String encodedPath = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(validPath.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    verifyLogEntry(logEntry, new Text(PROVIDER_ID + "/@" + validHost.replace('+', ':') + "/v2/"
        + "legacy/" + validUUID + "/" + encodedPath));
  }

  @Test
  public void testFromMetadata() {
    var columnQualifier = PROVIDER_ID + "/@" + validHost.replace('+', ':') + "/" + validPath;
    var logEntry = LogEntry.fromMetaWalEntry(
        new SimpleImmutableEntry<>(new Key("1<", LogColumnFamily.STR_NAME, columnQualifier), null));
    verifyLogEntry(logEntry, new Text(columnQualifier));
  }

  @Test
  public void testProviderIdentityFromMetadata() {
    var entry = LogEntry.fromPath("example", validPath);
    var qualifier = entry.getColumnQualifier();
    var restored = LogEntry.fromMetaWalEntry(new SimpleImmutableEntry<>(
        new Key("1<", LogColumnFamily.STR_NAME, qualifier.toString()), new Value()));

    assertEquals("example", restored.getProviderId());
    assertEquals(new Path(validPath), restored.getPath());
    assertEquals(entry, restored);
    assertEquals(PROVIDER_ID, LogEntry.fromPath(PROVIDER_ID, validPath).getProviderId());
  }

  @Test
  public void testOpaqueReferenceRoundTrip() {
    String reference = "memory://wal/log?uuid=" + validUUID + "&part=a/b,c|d";
    LogEntry entry = LogEntry.fromReference(PROVIDER_ID, reference, validUUID,
        HostAndPort.fromString("localhost:9997"));
    var restored = LogEntry.fromMetaWalEntry(new SimpleImmutableEntry<>(
        new Key("1<", LogColumnFamily.STR_NAME, entry.getColumnQualifier().toString()),
        new Value()));

    assertEquals(reference, restored.getReference());
    assertEquals(validUUID, restored.getUniqueID());
    assertEquals(PROVIDER_ID, restored.getProviderId());
    assertEquals(entry, restored);
    assertThrows(IllegalStateException.class, restored::getPath);
  }

  @Test
  public void testOpaqueReferenceMustContainUuid() {
    assertThrows(IllegalArgumentException.class,
        () -> LogEntry.fromReference(PROVIDER_ID, "memory://wal/log", validUUID));
  }

  @Test
  public void testLegacyMetadataQualifierRejected() {
    var legacyEntry = new SimpleImmutableEntry<>(
        new Key("1<", LogColumnFamily.STR_NAME, "-/" + validPath), new Value());
    assertThrows(IllegalArgumentException.class, () -> LogEntry.fromMetaWalEntry(legacyEntry));
  }

  @Test
  public void testProviderIdMustBeAlphanumeric() {
    assertThrows(IllegalArgumentException.class, () -> LogEntry.fromPath("provider-id", validPath));
    assertThrows(IllegalArgumentException.class, () -> LogEntry.fromPath("provider_id", validPath));
    assertThrows(IllegalArgumentException.class, () -> LogEntry.fromPath("provider.id", validPath));
  }

  @Test
  public void testForServer() {
    var logEntry = LogEntry.forServer(PROVIDER_ID, "viewfs:/a/accumulo",
        HostAndPort.fromString(validHost.replace('+', ':')));
    assertTrue(logEntry.getPath().toUri().getScheme() != null);

    var roundTripped = LogEntry.fromMetaWalEntry(new SimpleImmutableEntry<>(
        new Key("1<", LogColumnFamily.STR_NAME, logEntry.getColumnQualifier().toString()), null));
    assertEquals(logEntry, roundTripped);
    assertEquals(logEntry.getPath(), roundTripped.getPath());
    assertEquals(logEntry.getUniqueID(), roundTripped.getUniqueID());
    assertEquals(HostAndPort.fromString(validHost.replace('+', ':')), logEntry.getTServerAddress());
    assertEquals("viewfs:/a/accumulo/wal/" + roundTripped.getUniqueID(),
        logEntry.getPath().toString());
  }

  @Test
  public void testForServerRequiresQualifiedVolume() {
    assertThrows(IllegalArgumentException.class, () -> LogEntry.forServer(PROVIDER_ID, "/accumulo",
        HostAndPort.fromString("localhost:9997")));
  }

  // helper for testing build from constructor or from metadata
  private void verifyLogEntry(LogEntry logEntry, Text expectedColumnQualifier) {
    assertEquals(validPath, logEntry.toString());
    assertEquals(validPath, logEntry.getPath().toString());
    assertEquals(HostAndPort.fromString(validHost.replace('+', ':')), logEntry.getTServerAddress());
    assertEquals(expectedColumnQualifier, logEntry.getColumnQualifier());
    assertEquals(validUUID, logEntry.getUniqueID());
  }

  @Test
  public void testEquals() {
    LogEntry one = LogEntry.fromPath(PROVIDER_ID, validPath);
    LogEntry two = LogEntry.fromPath(PROVIDER_ID, validPath);

    assertNotSame(one, two);
    assertEquals(one.toString(), two.toString());
    assertEquals(one.getPath(), two.getPath());
    assertEquals(one.getTServerAddress(), two.getTServerAddress());
    assertEquals(one.getColumnQualifier(), two.getColumnQualifier());
    assertEquals(one.getUniqueID(), two.getUniqueID());
    assertEquals(one, two);

    assertEquals(one, one);
    assertEquals(two, two);
  }

  @Test
  public void testValidPaths() {
    var validPath1 = "file:///accumulo/wal/" + validHost + "/" + validUUID;
    var validPath2 = "hdfs://nn1/accumulo/wal/" + validHost + "/" + validUUID;

    for (String path : new String[] {validPath1, validPath2}) {
      assertDoesNotThrow(() -> LogEntry.fromPath(PROVIDER_ID, path));
    }
  }

  @Test
  public void testRelativePathsAreInvalid() {
    for (String path : new String[] {validHost + "/" + validUUID,
        "file:accumulo/wal/" + validHost + "/" + validUUID}) {
      assertThrows(IllegalArgumentException.class, () -> LogEntry.fromPath(PROVIDER_ID, path));
    }
  }

  @Test
  public void testRelativeMetadataPathIsInvalid() {
    var qualifier = "prefix/@" + validHost.replace('+', ':') + "/relative/" + validUUID;
    var entry = new SimpleImmutableEntry<Key,Value>(
        new Key("1<", LogColumnFamily.STR_NAME, qualifier), null);
    assertThrows(IllegalArgumentException.class, () -> LogEntry.fromMetaWalEntry(entry));
  }

  @Test
  public void testBadPathLength() {
    Stream.of("file:///accumulo/wal/foo", "file:///accumulo/wal/", "file:///accumulo/wal")
        .forEach(badPath -> {
          var e = assertThrows(IllegalArgumentException.class,
              () -> LogEntry.fromPath(PROVIDER_ID, badPath));
          assertTrue(e.getMessage().contains("Expected valid UUID"));
        });
  }

  @Test
  public void testPathDoesNotRequireTserverAddress() {
    var logEntry =
        LogEntry.fromPath(PROVIDER_ID, "file:///accumulo/wal/provider-shard/" + validUUID);
    assertEquals(validUUID, logEntry.getUniqueID());
    assertTrue(logEntry.getTServerAddress() == null);
  }

  @Test
  public void testInvalidUUID() {
    var badUUID = "badUUID";
    var pathWithBadUUID = "file:///accumulo/wal/" + badUUID;
    var e = assertThrows(IllegalArgumentException.class,
        () -> LogEntry.fromPath(PROVIDER_ID, pathWithBadUUID));
    assertTrue(e.getMessage().contains("Expected valid UUID as the final path component"));
  }

}
