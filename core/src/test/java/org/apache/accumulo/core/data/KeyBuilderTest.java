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
package org.apache.accumulo.core.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class KeyBuilderTest {

  private static final byte[] EMPTY_BYTES = new byte[0];
  byte[] rowBytes = "row".getBytes(UTF_8);
  byte[] familyBytes = "family".getBytes(UTF_8);
  byte[] qualifierBytes = "qualifier".getBytes(UTF_8);
  byte[] visibilityBytes = "visibility".getBytes(UTF_8);
  Text rowText = new Text(rowBytes);
  Text familyText = new Text(familyBytes);
  Text qualifierText = new Text(qualifierBytes);
  Text visibilityText = new Text(visibilityBytes);
  ColumnVisibility visibilityVisibility = new ColumnVisibility(visibilityBytes);

  @Test
  public void testKeyBuildingFromRow() {
    Key keyBuilt = Key.builder().row("foo").build();
    Key keyExpected = new Key("foo");
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamily() {
    Key keyBuilt = Key.builder().row("foo").family("bar").build();
    Key keyExpected = new Key("foo", "bar");
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifier() {
    Key keyBuilt = Key.builder().row("foo").family("bar").qualifier("baz").build();
    Key keyExpected = new Key("foo", "bar", "baz");
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibility() {
    Key keyBuilt = Key.builder().row("foo").family("bar").qualifier("baz").visibility("v").build();
    Key keyExpected = new Key("foo", "bar", "baz", "v");
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestamp() {
    Key keyBuilt = Key.builder().row("foo").family("bar").qualifier("baz").visibility("v")
        .timestamp(1L).build();
    Key keyExpected = new Key("foo", "bar", "baz", "v", 1L);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampDeleted() {
    Key keyBuilt = Key.builder().row("foo").family("bar").qualifier("baz").visibility("v")
        .timestamp(10L).deleted(true).build();
    Key keyExpected = new Key("foo", "bar", "baz", "v", 10L);
    keyExpected.setDeleted(true);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowVisibility() {
    Key keyBuilt = Key.builder().row("foo").visibility("v").build();
    Key keyExpected = new Key("foo", "", "", "v");
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibility() {
    Key keyBuilt = Key.builder().row("foo").family("bar").visibility("v").build();
    Key keyExpected = new Key("foo", "bar", "", "v");
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void textKeyBuildingFromRowTimestamp() {
    Key keyBuilt = Key.builder().row("foo").timestamp(3L).build();
    Key keyExpected = new Key("foo");
    keyExpected.setTimestamp(3L);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).build();
    Key keyExpected = new Key(rowBytes, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).family(familyBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierBytes() {
    Key keyBuilt =
        Key.builder().row(rowBytes).family(familyBytes).qualifier(qualifierBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, EMPTY_BYTES, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).family(familyBytes).qualifier(qualifierBytes)
        .visibility(visibilityBytes).build();
    Key keyExpected =
        new Key(rowBytes, familyBytes, qualifierBytes, visibilityBytes, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).family(familyBytes).qualifier(qualifierBytes)
        .visibility(visibilityBytes).timestamp(1L).build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, visibilityBytes, 1L);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampDeletedBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).family(familyBytes).qualifier(qualifierBytes)
        .visibility(visibilityBytes).timestamp(10L).deleted(true).build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, visibilityBytes, 10L);
    keyExpected.setDeleted(true);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowVisibilityBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).visibility(visibilityBytes).build();
    Key keyExpected = new Key(rowBytes, EMPTY_BYTES, EMPTY_BYTES, visibilityBytes, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibilityBytes() {
    Key keyBuilt =
        Key.builder().row(rowBytes).family(familyBytes).visibility(visibilityBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, EMPTY_BYTES, visibilityBytes, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void textKeyBuildingFromRowTimestampBytes() {
    Key keyBuilt = Key.builder().row(rowBytes).timestamp(3L).build();
    Key keyExpected = new Key(rowBytes, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
    keyExpected.setTimestamp(3L);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowText() {
    Key keyBuilt = Key.builder().row(rowText).build();
    Key keyExpected = new Key(rowText);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyText() {
    Key keyBuilt = Key.builder().row(rowText).family(familyText).build();
    Key keyExpected = new Key(rowText, familyText);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierText() {
    Key keyBuilt = Key.builder().row(rowText).family(familyText).qualifier(qualifierText).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityText() {
    Key keyBuilt = Key.builder().row(rowText).family(familyText).qualifier(qualifierText)
        .visibility(visibilityText).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText, visibilityText);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampText() {
    Key keyBuilt = Key.builder().row(rowText).family(familyText).qualifier(qualifierText)
        .visibility(visibilityText).timestamp(1L).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText, visibilityText, 1L);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampDeletedText() {
    Key keyBuilt = Key.builder().row(rowText).family(familyText).qualifier(qualifierText)
        .visibility(visibilityText).timestamp(10L).deleted(true).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText, visibilityText, 10L);
    keyExpected.setDeleted(true);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowVisibilityText() {
    Key keyBuilt = Key.builder().row(rowText).visibility(visibilityText).build();
    Key keyExpected = new Key(rowText, new Text(), new Text(), visibilityText);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibilityText() {
    Key keyBuilt = Key.builder().row(rowText).family(familyText).visibility(visibilityText).build();
    Key keyExpected = new Key(rowText, familyText, new Text(), visibilityText);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibilityVisibility() {
    Key keyBuilt =
        Key.builder().row(rowText).family(familyText).visibility(visibilityVisibility).build();
    Key keyExpected =
        new Key(rowText, familyText, new Text(), visibilityVisibility, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingFromRowTimestampText() {
    Key keyBuilt = Key.builder().row(rowText).timestamp(3L).build();
    Key keyExpected = new Key(rowText);
    keyExpected.setTimestamp(3L);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingReusingBytes() {
    byte[] reuse = {1, 2, 3};
    KeyBuilder.Build keyBuilder = Key.builder(false).row(reuse);
    Key keyBuilt = keyBuilder.build();
    assertSame(reuse, keyBuilt.getRowBytes());
  }

  @Test
  public void testKeyBuildingCopyBytes() {
    byte[] reuse = {1, 2, 3};
    KeyBuilder.Build keyBuilder = Key.builder(true).row(reuse);
    Key keyBuilt = keyBuilder.build();
    assertNotEquals(reuse, keyBuilt.getRowBytes());
    Key keyBuilt2 = keyBuilder.build();
    assertNotEquals(reuse, keyBuilt2.getRowBytes());
  }

  @Test
  public void testKeyHeterogeneous() {
    Key keyBuilt = Key.builder().row(rowText).family(familyBytes).qualifier("foo").build();
    Text fooText = new Text("foo");
    Key keyExpected =
        new Key(rowText.getBytes(), 0, rowText.getLength(), familyBytes, 0, familyBytes.length,
            fooText.getBytes(), 0, fooText.getLength(), EMPTY_BYTES, 0, 0, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyUsingSubsetOfBytes() {
    Key keyBuilt = Key.builder().row(rowBytes, 0, rowBytes.length - 1).build();
    Key keyExpected = new Key(rowBytes, 0, rowBytes.length - 1, EMPTY_BYTES, 0, 0, EMPTY_BYTES, 0,
        0, EMPTY_BYTES, 0, 0, Long.MAX_VALUE);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingWithMultipleTimestamps() {
    Key keyBuilt = Key.builder().row("r").timestamp(44).timestamp(99).build();
    Key keyExpected = new Key("r", "", "", 99);
    assertEquals(keyExpected, keyBuilt);
  }

  @Test
  public void testKeyBuildingWithMultipleDeleted() {
    Key keyBuilt = Key.builder().row("r").deleted(true).deleted(false).build();
    Key keyExpected = new Key("r");
    keyExpected.setDeleted(false);
    assertEquals(keyExpected, keyBuilt);
  }

  /**
   * Tests bug where a String of 10 chars or longer was being encoded incorrectly.
   */
  @Test
  public void test10CharactersBug() {
    Key keyBuilt1 = Key.builder().row(rowText).family("1234567890").build();
    Key keyBuilt2 = Key.builder().row(rowText).family(new Text("1234567890")).build();
    assertEquals(keyBuilt1, keyBuilt2);
  }
}
