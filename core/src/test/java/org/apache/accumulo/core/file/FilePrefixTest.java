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
package org.apache.accumulo.core.file;

import static org.apache.accumulo.core.file.FilePrefix.BULK_IMPORT;
import static org.apache.accumulo.core.file.FilePrefix.COMPACTION;
import static org.apache.accumulo.core.file.FilePrefix.FLUSH;
import static org.apache.accumulo.core.file.FilePrefix.FULL_COMPACTION;
import static org.apache.accumulo.core.file.FilePrefix.MERGING_MINOR_COMPACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class FilePrefixTest {

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
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix('*'));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix('c'));
    assertEquals(COMPACTION, FilePrefix.fromPrefix('C'));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix('a'));
    assertEquals(FULL_COMPACTION, FilePrefix.fromPrefix('A'));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix('f'));
    assertEquals(FLUSH, FilePrefix.fromPrefix('F'));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix('i'));
    assertEquals(BULK_IMPORT, FilePrefix.fromPrefix('I'));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix('m'));
    assertEquals(MERGING_MINOR_COMPACTION, FilePrefix.fromPrefix('M'));
  }

  @Test
  public void fromFileName() {
    assertThrows(NullPointerException.class, () -> FilePrefix.fromFileName(null));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName(""));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("*file.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("cfile.rf"));
    assertEquals(COMPACTION, FilePrefix.fromFileName("Cfile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("afile.rf"));
    assertEquals(FULL_COMPACTION, FilePrefix.fromFileName("Afile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("ffile.rf"));
    assertEquals(FLUSH, FilePrefix.fromFileName("Ffile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("ifile.rf"));
    assertEquals(BULK_IMPORT, FilePrefix.fromFileName("Ifile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("mfile.rf"));
    assertEquals(MERGING_MINOR_COMPACTION, FilePrefix.fromFileName("Mfile.rf"));

  }

}
