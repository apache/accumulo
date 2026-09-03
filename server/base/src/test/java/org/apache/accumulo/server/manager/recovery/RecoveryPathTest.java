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
package org.apache.accumulo.server.manager.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class RecoveryPathTest {

  private static final String UUID = "2d961760-db4f-47eb-97fe-d283331ec254";

  private static void assertBothValid(String input, String expectedOutput) {
    Path hadoopResult = getRecoveryPath(new Path(input));
    assertEquals(expectedOutput, hadoopResult.toString(),
        "getRecoveryPath mismatch for : " + input);

    String stringResult = RecoveryPath.transformToRecoveryPath(input);
    assertEquals(expectedOutput, stringResult, "validatePath mismatch for : " + input);
  }

  private static void assertBothInvalid(String input, String messageContains) {
    var error = assertThrows(IllegalArgumentException.class, () -> getRecoveryPath(new Path(input)),
        "getRecoveryPath should throw for: " + input);
    assertTrue(error.getMessage().contains(messageContains), "getRecoveryPath message '"
        + error.getMessage() + "' does not contain '" + messageContains);
    var err = assertThrows(IllegalArgumentException.class,
        () -> RecoveryPath.transformToRecoveryPath(input),
        "getRecoveryPath should throw for: " + input);
    assertTrue(err.getMessage().contains(messageContains),
        "validatePath message '" + err.getMessage() + "' does not contain '" + messageContains);
  }

  @Test
  public void testValidHdfsPathConversions() {

    assertBothValid("hdfs://nn1/accumulo/wal/localhost+9997/" + UUID,
        "hdfs://nn1/accumulo/recovery/" + UUID);

    // included port
    assertBothValid("hdfs://nn1:9000/accumulo/wal/localhost+9997/" + UUID,
        "hdfs://nn1:9000/accumulo/recovery/" + UUID);

    // IP
    assertBothValid("hdfs://192.168.1.1:9000/accumulo/wal/192.168.1.2+9997/" + UUID,
        "hdfs://192.168.1.1:9000/accumulo/recovery/" + UUID);

    // alternate server hostname
    assertBothValid("hdfs://namenode/accumulo/wal/my-host.example.com+9997/" + UUID,
        "hdfs://namenode/accumulo/recovery/" + UUID);

    // viewfs vs hdfs
    assertBothValid("viewfs://clusterX/accumulo/wal/localhost+9997/" + UUID,
        "viewfs://clusterX/accumulo/recovery/" + UUID);

    // Deep basePath
    assertBothValid("hdfs://nn1/a/b/c/wal/localhost+9997/" + UUID,
        "hdfs://nn1/a/b/c/recovery/" + UUID);

  }

  @Test
  public void testValidFilePathConversions() {
    assertBothValid("file:///tmp/accumulo/wal/localhost+9997/" + UUID,
        "file:/tmp/accumulo/recovery/" + UUID);
  }

  @Test
  public void testInvalidPaths() {
    // No scheme and relative
    assertBothInvalid("accumulo/wal/localhost+9997/" + UUID, "Bad path");

    // No scheme and absolute
    assertBothInvalid("/accumulo/wal/localhost+9997/" + UUID, "Bad path");

    // Only contains uuid under authority
    assertBothInvalid("hdfs://nn1/" + UUID, "Bad path");
    // min depth less than 3
    assertBothInvalid("hdfs://nn1/wal/" + UUID, "Bad path");

    assertBothInvalid("hdfs://nn1/accumulo/wal/" + UUID, "missing server component");

    assertBothInvalid("hdfs://nn1/accumulo/recovery/localhost+9997/" + UUID,
        "missing wal directory component");

    // Invalid WAL dir case
    assertBothInvalid("hdfs://nn1/accumulo/WAL/localhost+9997/" + UUID,
        "missing wal directory component");
  }

  @Test
  public void testVerifyResultsAreIdentical() {
    String[] validPaths = {"hdfs://nn1:9000/accumulo/wal/localhost+9997/" + UUID,
        "file:///tmp/acc/wal/host+9997/" + UUID, "hdfs://nn1/a/b/c/wal/s+1/" + UUID,
        "viewfs://c/base/wal/h+2/" + UUID,};

    for (String path : validPaths) {
      String hadoopResult = getRecoveryPath(new Path(path)).toString();
      String stringResult = RecoveryPath.transformToRecoveryPath(path);
      assertEquals(hadoopResult, stringResult, "Results diverge for path: " + path);
    }
  }

  @Test
  public void testRejectionsAreIdentical() {
    String[] invalidPaths = {"/accumulo/wal/localhost+9997/" + UUID, // no scheme
        "hdfs://nn1/wal/" + UUID, // too shallow
        "hdfs://nn1/acc/tables/h+1/" + UUID, // wrong directory
    };

    for (String path : invalidPaths) {
      assertThrows(IllegalArgumentException.class, () -> getRecoveryPath(new Path(path)),
          "getRecoveryPath should reject: " + path);
      assertThrows(IllegalArgumentException.class, () -> RecoveryPath.transformToRecoveryPath(path),
          "validatePath should reject: " + path);
    }
  }

  // given a wal path, transform it to a recovery path
  static Path getRecoveryPath(Path walPath) {
    if (walPath.depth() >= 3 && walPath.toUri().getScheme() != null) {
      // it's a fully qualified path
      String uuid = walPath.getName();
      // drop uuid
      walPath = walPath.getParent();

      // expect and drop the server component
      if (walPath.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException("Bath path " + walPath + " (missing server component)");
      }
      walPath = walPath.getParent();

      // expect and drop the wal component
      if (!walPath.getName().equals(FileType.WAL.getDirectory())) {
        throw new IllegalArgumentException(
            "Bad path " + walPath + " (missing wal directory component)");
      }
      walPath = walPath.getParent();

      // create new path in recovery directory that is a sibling to the wal directory (same volume),
      // without the server component
      walPath = new Path(walPath, FileType.RECOVERY.getDirectory());
      walPath = new Path(walPath, uuid);

      return walPath;
    }

    throw new IllegalArgumentException("Bad path " + walPath);
  }

}
