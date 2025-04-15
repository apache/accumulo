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

import static org.apache.accumulo.core.file.FilePrefix.ALL;
import static org.apache.accumulo.core.file.FilePrefix.BULK_IMPORT;
import static org.apache.accumulo.core.file.FilePrefix.MAJOR_COMPACTION;
import static org.apache.accumulo.core.file.FilePrefix.MAJOR_COMPACTION_ALL_FILES;
import static org.apache.accumulo.core.file.FilePrefix.MERGING_MINOR_COMPACTION;
import static org.apache.accumulo.core.file.FilePrefix.MINOR_COMPACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

public class FilePrefixTest {

  @Test
  public void testFromPrefix() {
    assertThrows(NullPointerException.class, () -> FilePrefix.fromPrefix(null));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix(""));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromPrefix("AB"));
    assertEquals(MAJOR_COMPACTION, FilePrefix.fromPrefix("c"));
    assertEquals(MAJOR_COMPACTION, FilePrefix.fromPrefix("C"));
    assertEquals(MAJOR_COMPACTION_ALL_FILES, FilePrefix.fromPrefix("a"));
    assertEquals(MAJOR_COMPACTION_ALL_FILES, FilePrefix.fromPrefix("A"));
    assertEquals(MINOR_COMPACTION, FilePrefix.fromPrefix("f"));
    assertEquals(MINOR_COMPACTION, FilePrefix.fromPrefix("F"));
    assertEquals(BULK_IMPORT, FilePrefix.fromPrefix("i"));
    assertEquals(BULK_IMPORT, FilePrefix.fromPrefix("I"));
    assertEquals(MERGING_MINOR_COMPACTION, FilePrefix.fromPrefix("m"));
    assertEquals(MERGING_MINOR_COMPACTION, FilePrefix.fromPrefix("M"));
    assertThrows(IllegalArgumentException.class, () -> {
      FilePrefix.fromPrefix("B");
    });
  }

  @Test
  public void testCreateFileName() {
    assertThrows(NullPointerException.class, () -> MINOR_COMPACTION.createFileName(null));
    assertThrows(IllegalArgumentException.class, () -> MINOR_COMPACTION.createFileName(""));
    assertThrows(IllegalStateException.class, () -> ALL.createFileName("file.rf"));
    assertThrows(IllegalStateException.class,
        () -> MERGING_MINOR_COMPACTION.createFileName("file.rf"));
    assertEquals("Afile.rf", MAJOR_COMPACTION_ALL_FILES.createFileName("file.rf"));
    assertEquals("Cfile.rf", MAJOR_COMPACTION.createFileName("file.rf"));
    assertEquals("Ffile.rf", MINOR_COMPACTION.createFileName("file.rf"));
    assertEquals("Ifile.rf", BULK_IMPORT.createFileName("file.rf"));
  }

  @Test
  public void fromFileName() {
    assertThrows(NullPointerException.class, () -> FilePrefix.fromFileName(null));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName(""));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("*file.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("cfile.rf"));
    assertEquals(MAJOR_COMPACTION, FilePrefix.fromFileName("Cfile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("afile.rf"));
    assertEquals(MAJOR_COMPACTION_ALL_FILES, FilePrefix.fromFileName("Afile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("ffile.rf"));
    assertEquals(MINOR_COMPACTION, FilePrefix.fromFileName("Ffile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("ifile.rf"));
    assertEquals(BULK_IMPORT, FilePrefix.fromFileName("Ifile.rf"));
    assertThrows(IllegalArgumentException.class, () -> FilePrefix.fromFileName("mfile.rf"));
    assertEquals(MERGING_MINOR_COMPACTION, FilePrefix.fromFileName("Mfile.rf"));

  }

  @Test
  public void testFromList() {
    assertEquals(EnumSet.noneOf(FilePrefix.class), FilePrefix.typesFromList(""));
    assertEquals(EnumSet.of(ALL), FilePrefix.typesFromList("*"));
    assertEquals(EnumSet.of(ALL), FilePrefix.typesFromList("*, A"));
    assertEquals(EnumSet.of(MAJOR_COMPACTION, MAJOR_COMPACTION_ALL_FILES),
        FilePrefix.typesFromList("C, A"));
  }

}
