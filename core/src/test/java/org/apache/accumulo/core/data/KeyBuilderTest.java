package org.apache.accumulo.core.data;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;

public class KeyBuilderTest {

  private static final byte EMPTY_BYTES[] = new byte[0];
  byte[] rowBytes = "row".getBytes();
  byte[] familyBytes = "family".getBytes();
  byte[] qualifierBytes = "qualifier".getBytes();
  byte[] visibilityBytes = "visibility".getBytes();
  Text rowText = new Text(rowBytes);
  Text familyText = new Text(familyBytes);
  Text qualifierText = new Text(qualifierBytes);
  Text visibilityText = new Text(visibilityBytes);

  @Test
  public void testKeyBuildingFromRow() {
    Key keyBuilt  = KeyBuilder.row("foo").build();
    Key keyExpected = new Key("foo");
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamily() {
    Key keyBuilt = KeyBuilder.row("foo").columnFamily("bar").build();
    Key keyExpected = new Key("foo", "bar");
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifier() {
    Key keyBuilt = KeyBuilder.row("foo").columnFamily("bar").columnQualifier("baz").build();
    Key keyExpected = new Key("foo", "bar", "baz");
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibility() {
    Key keyBuilt  = KeyBuilder.row("foo").columnFamily("bar").columnQualifier("baz").columnVisibility("v").build();
    Key keyExpected = new Key("foo", "bar", "baz", "v");
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestamp() {
    Key keyBuilt = KeyBuilder.row("foo").columnFamily("bar").columnQualifier("baz").columnVisibility("v").timestamp(1L).build();
    Key keyExpected = new Key("foo", "bar", "baz", "v", 1L);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampDeleted() {
    Key keyBuilt =
        KeyBuilder
            .row("foo")
            .columnFamily("bar")
            .columnQualifier("baz")
            .columnVisibility("v")
            .timestamp(10L)
            .deleted(true)
            .build();
    Key keyExpected = new Key("foo", "bar", "baz", "v", 10L);
    keyExpected.setDeleted(true);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowVisibility() {
    Key keyBuilt = KeyBuilder.row("foo").columnVisibility("v").build();
    Key keyExpected = new Key("foo", "", "", "v");
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibility() {
    Key keyBuilt = KeyBuilder.row("foo").columnFamily("bar").columnVisibility("v").build();
    Key keyExpected = new Key("foo", "bar", "", "v");
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void textKeyBuildingFromRowTimestamp() {
    Key keyBuilt = KeyBuilder.row("foo").timestamp(3L).build();
    Key keyExpected = new Key("foo");
    keyExpected.setTimestamp(3L);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowBytes() {
    Key keyBuilt  = KeyBuilder.row(rowBytes).build();
    Key keyExpected = new Key(rowBytes, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyBytes() {
    Key keyBuilt = KeyBuilder.row(rowBytes).columnFamily(familyBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierBytes() {
    Key keyBuilt = KeyBuilder.row(rowBytes).columnFamily(familyBytes).columnQualifier(qualifierBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, EMPTY_BYTES, Long.MAX_VALUE);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityBytes() {
    Key keyBuilt  = KeyBuilder.row(rowBytes).columnFamily(familyBytes).columnQualifier(qualifierBytes).columnVisibility(visibilityBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, visibilityBytes, Long.MAX_VALUE);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampBytes() {
    Key keyBuilt = KeyBuilder.row(rowBytes).columnFamily(familyBytes).columnQualifier(qualifierBytes).columnVisibility(visibilityBytes).timestamp(1L).build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, visibilityBytes, 1L);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampDeletedBytes() {
    Key keyBuilt =
        KeyBuilder
            .row(rowBytes)
            .columnFamily(familyBytes)
            .columnQualifier(qualifierBytes)
            .columnVisibility(visibilityBytes)
            .timestamp(10L)
            .deleted(true)
            .build();
    Key keyExpected = new Key(rowBytes, familyBytes, qualifierBytes, visibilityBytes, 10L);
    keyExpected.setDeleted(true);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowVisibilityBytes() {
    Key keyBuilt = KeyBuilder.row(rowBytes).columnVisibility(visibilityBytes).build();
    Key keyExpected = new Key(rowBytes, EMPTY_BYTES, EMPTY_BYTES, visibilityBytes, Long.MAX_VALUE);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibilityBytes() {
    Key keyBuilt = KeyBuilder.row(rowBytes).columnFamily(familyBytes).columnVisibility(visibilityBytes).build();
    Key keyExpected = new Key(rowBytes, familyBytes, EMPTY_BYTES, visibilityBytes, Long.MAX_VALUE);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void textKeyBuildingFromRowTimestampBytes() {
    Key keyBuilt = KeyBuilder.row(rowBytes).timestamp(3L).build();
    Key keyExpected = new Key(rowBytes, EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE);
    keyExpected.setTimestamp(3L);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowText() {
    Key keyBuilt  = KeyBuilder.row(rowText).build();
    Key keyExpected = new Key(rowText);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyText() {
    Key keyBuilt = KeyBuilder.row(rowText).columnFamily(familyText).build();
    Key keyExpected = new Key(rowText, familyText);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierText() {
    Key keyBuilt = KeyBuilder.row(rowText).columnFamily(familyText).columnQualifier(qualifierText).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityText() {
    Key keyBuilt  = KeyBuilder.row(rowText).columnFamily(familyText).columnQualifier(qualifierText).columnVisibility(visibilityText).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText, visibilityText);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampText() {
    Key keyBuilt = KeyBuilder.row(rowText).columnFamily(familyText).columnQualifier(qualifierText).columnVisibility(visibilityText).timestamp(1L).build();
    Key keyExpected = new Key(rowText, familyText, qualifierText, visibilityText, 1L);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyQualifierVisibilityTimestampDeletedText() {
    Key keyBuilt =
        KeyBuilder
            .row(rowText)
            .columnFamily(familyText)
            .columnQualifier(qualifierText)
            .columnVisibility(visibilityText)
            .timestamp(10L)
            .deleted(true)
            .build();
    Key keyExpected = new Key(rowText, familyText, qualifierText, visibilityText, 10L);
    keyExpected.setDeleted(true);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowVisibilityText() {
    Key keyBuilt = KeyBuilder.row(rowText).columnVisibility(visibilityText).build();
    Key keyExpected = new Key(rowText, new Text(), new Text(), visibilityText);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void testKeyBuildingFromRowFamilyVisibilityText() {
    Key keyBuilt = KeyBuilder.row(rowText).columnFamily(familyText).columnVisibility(visibilityText).build();
    Key keyExpected = new Key(rowText, familyText, new Text(), visibilityText);
    assertEquals(keyBuilt, keyExpected);
  }

  @Test
  public void textKeyBuildingFromRowTimestampText() {
    Key keyBuilt = KeyBuilder.row(rowText).timestamp(3L).build();
    Key keyExpected = new Key(rowText);
    keyExpected.setTimestamp(3L);
    assertEquals(keyBuilt, keyExpected);
  }

}