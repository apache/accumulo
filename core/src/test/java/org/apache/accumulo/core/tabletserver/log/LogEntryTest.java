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
import java.util.AbstractMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class LogEntryTest {

  private final HostAndPort validHost = HostAndPort.fromParts("default", 8080);
  private final UUID validUUID = UUID.randomUUID();
  private final String validFilename = validHost + "/" + validUUID;

  @Test
  public void test() throws Exception {
    assertEquals(new Text("log"), MetadataSchema.TabletsSection.LogColumnFamily.NAME);

    // test from constructor
    LogEntry one = new LogEntry(validFilename);
    assertEquals(validFilename, one.toString());
    assertEquals(validFilename, one.getLogReference());
    assertEquals(new Text("-/" + validFilename), one.getColumnQualifier());
    assertEquals(validUUID.toString(), one.getUniqueID());

    // test from metadata entry
    LogEntry two = LogEntry.fromMetaWalEntry(new AbstractMap.SimpleImmutableEntry<>(
        new Key(new Text("1<"), new Text("log"), one.getColumnQualifier()), new Value("unused")));
    assertNotSame(one, two);
    assertEquals(one.toString(), two.toString());
    assertEquals(one.getLogReference(), two.getLogReference());
    assertEquals(one.getColumnQualifier(), two.getColumnQualifier());
    assertEquals(one.getUniqueID(), two.getUniqueID());
    assertEquals(one, two);
  }

  @Test
  public void testEquals() {
    LogEntry one = new LogEntry(validFilename);
    LogEntry two = new LogEntry(validFilename);

    assertNotSame(one, two);
    assertEquals(one.toString(), two.toString());
    assertEquals(one.getLogReference(), two.getLogReference());
    assertEquals(one.getColumnQualifier(), two.getColumnQualifier());
    assertEquals(one.getUniqueID(), two.getUniqueID());
    assertEquals(one, two);

    assertEquals(one, one);
    assertEquals(two, two);
  }

  @Nested
  class ValidateFilePath {

    @Test
    public void testValidPaths() {
      Path validPath = Path.of(validHost.toString(), validUUID.toString());
      Path validPath2 = Path.of("dir1", validPath.toString());
      Path validPath3 = Path.of("dir2", validPath2.toString());

      Stream.of(validPath, validPath2, validPath3).map(Path::toString)
          .forEach(validFilePath -> assertDoesNotThrow(() -> new LogEntry(validFilePath)));
    }

    @Test
    public void testBadPathLength() {
      List<String> badFilePaths = List.of("foo", "", validHost.toString());

      for (String badFilePath : badFilePaths) {
        IllegalArgumentException iae =
            assertThrows(IllegalArgumentException.class, () -> new LogEntry(badFilePath));
        assertTrue(iae.getMessage().contains("The path should at least contain tserver/UUID."));
      }
    }

    @Test
    public void testInvalidHostPort() {
      final String badHostAndPort = "default:badPort";
      final Path badFilepathHostPort = Path.of(badHostAndPort, validUUID.toString());

      IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
          () -> new LogEntry(badFilepathHostPort.toString()));
      assertTrue(
          iae.getMessage().contains("Expected format: host:port. Found '" + badHostAndPort + "'"));
    }

    @Test
    public void testInvalidUUID() {
      final String badUUID = "badUUID";
      String filePathWithBadUUID = Path.of(validHost.toString(), badUUID).toString();

      IllegalArgumentException iae =
          assertThrows(IllegalArgumentException.class, () -> new LogEntry(filePathWithBadUUID));
      assertTrue(iae.getMessage().contains("Expected valid UUID. Found '" + badUUID + "'"));
    }
  }

}
