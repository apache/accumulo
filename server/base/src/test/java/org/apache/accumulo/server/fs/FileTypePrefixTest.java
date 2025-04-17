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
package org.apache.accumulo.server.fs;

import static org.apache.accumulo.server.fs.FileTypePrefix.BULK_IMPORT;
import static org.apache.accumulo.server.fs.FileTypePrefix.COMPACTION;
import static org.apache.accumulo.server.fs.FileTypePrefix.FLUSH;
import static org.apache.accumulo.server.fs.FileTypePrefix.FULL_COMPACTION;
import static org.apache.accumulo.server.fs.FileTypePrefix.MERGING_MINOR_COMPACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class FileTypePrefixTest {

  @Test
  public void testCreateFileName() {
    assertThrows(NullPointerException.class, () -> FLUSH.createFileName(null));
    assertThrows(IllegalArgumentException.class, () -> FLUSH.createFileName(""));
    assertThrows(IllegalStateException.class,
        () -> MERGING_MINOR_COMPACTION.createFileName("file.rf"));
    assertEquals("Afile.rf", FULL_COMPACTION.createFileName("file.rf"));
    assertEquals("Cfile.rf", COMPACTION.createFileName("file.rf"));
    assertEquals("Ffile.rf", FLUSH.createFileName("file.rf"));
    assertEquals("Ifile.rf", BULK_IMPORT.createFileName("file.rf"));
  }

  @Test
  public void testFromPrefix() {
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromPrefix('*'));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromPrefix('c'));
    assertEquals(COMPACTION, FileTypePrefix.fromPrefix('C'));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromPrefix('a'));
    assertEquals(FULL_COMPACTION, FileTypePrefix.fromPrefix('A'));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromPrefix('f'));
    assertEquals(FLUSH, FileTypePrefix.fromPrefix('F'));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromPrefix('i'));
    assertEquals(BULK_IMPORT, FileTypePrefix.fromPrefix('I'));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromPrefix('m'));
    assertEquals(MERGING_MINOR_COMPACTION, FileTypePrefix.fromPrefix('M'));
  }

  @Test
  public void fromFileName() {
    assertThrows(NullPointerException.class, () -> FileTypePrefix.fromFileName(null));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName(""));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName("*file.rf"));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName("cfile.rf"));
    assertEquals(COMPACTION, FileTypePrefix.fromFileName("Cfile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName("afile.rf"));
    assertEquals(FULL_COMPACTION, FileTypePrefix.fromFileName("Afile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName("ffile.rf"));
    assertEquals(FLUSH, FileTypePrefix.fromFileName("Ffile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName("ifile.rf"));
    assertEquals(BULK_IMPORT, FileTypePrefix.fromFileName("Ifile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FileTypePrefix.fromFileName("mfile.rf"));
    assertEquals(MERGING_MINOR_COMPACTION, FileTypePrefix.fromFileName("Mfile.rf"));

  }

}
