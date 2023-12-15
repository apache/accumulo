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

import java.nio.file.Path;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class LogEntryTest {

  private final String validHost = "localhost+9997";
  private final UUID validUUID = UUID.randomUUID();
  private final String validFilename = "viewfs:/a/accumulo/wal/" + validHost + "/" + validUUID;

  @Test
  public void testColumnFamily() {
    assertEquals(new Text("log"), LogColumnFamily.NAME);
  }

  @Test
  public void testFromFilePath() throws Exception {
    var logEntry = LogEntry.fromFilePath(validFilename);
    verifyLogEntry(logEntry);
  }

  @Test
  public void testFromMetadata() throws Exception {
    var logEntry = LogEntry.fromMetaWalEntry(new SimpleImmutableEntry<>(
        new Key("1<", LogColumnFamily.STR_NAME, "-/" + validFilename), null));
    verifyLogEntry(logEntry);
  }

  // helper for testing build from constructor or from metadata
  private void verifyLogEntry(LogEntry logEntry) {
    assertEquals(validFilename, logEntry.toString());
    assertEquals(validFilename, logEntry.getFilePath());
    assertEquals(HostAndPort.fromString(validHost.replace('+', ':')), logEntry.getTServer());
    assertEquals(new Text("-/" + validFilename), logEntry.getColumnQualifier());
    assertEquals(validUUID, logEntry.getUniqueID());
  }

  @Test
  public void testEquals() {
    LogEntry one = LogEntry.fromFilePath(validFilename);
    LogEntry two = LogEntry.fromFilePath(validFilename);

    assertNotSame(one, two);
    assertEquals(one.toString(), two.toString());
    assertEquals(one.getFilePath(), two.getFilePath());
    assertEquals(one.getTServer(), two.getTServer());
    assertEquals(one.getColumnQualifier(), two.getColumnQualifier());
    assertEquals(one.getUniqueID(), two.getUniqueID());
    assertEquals(one, two);

    assertEquals(one, one);
    assertEquals(two, two);
  }

  @Test
  public void testValidPaths() {
    Path validPath = Path.of(validHost.toString(), validUUID.toString());
    Path validPath2 = Path.of("dir1", validPath.toString());
    Path validPath3 = Path.of("dir2", validPath2.toString());

    Stream.of(validPath, validPath2, validPath3).map(Path::toString)
        .forEach(validFilePath -> assertDoesNotThrow(() -> LogEntry.fromFilePath(validFilePath)));
  }

  @Test
  public void testBadPathLength() {
    List<String> badFilePaths = List.of("foo", "", validHost.toString());
    badFilePaths.forEach(badFilePath -> {
      var e =
          assertThrows(IllegalArgumentException.class, () -> LogEntry.fromFilePath(badFilePath));
      assertTrue(e.getMessage().contains("The path should end with tserver/UUID."));
    });
  }

  @Test
  public void testInvalidHostPort() {
    Stream.of("default:9997", "default+badPort")
        .map(badHostAndPort -> Path.of(badHostAndPort, validUUID.toString()))
        .forEach(badFilepathHostPort -> {
          var e = assertThrows(IllegalArgumentException.class,
              () -> LogEntry.fromFilePath(badFilepathHostPort.toString()));
          assertTrue(e.getMessage().contains(
              "Expected: host+port. Found '" + badFilepathHostPort.iterator().next() + "'"));
        });
  }

  @Test
  public void testInvalidUUID() {
    var badUUID = "badUUID";
    var filePathWithBadUUID = Path.of(validHost.toString(), badUUID).toString();
    var e = assertThrows(IllegalArgumentException.class,
        () -> LogEntry.fromFilePath(filePathWithBadUUID));
    assertTrue(e.getMessage().contains("Expected valid UUID. Found '" + badUUID + "'"));
  }

}
