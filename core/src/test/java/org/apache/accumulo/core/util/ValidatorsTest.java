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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.function.Consumer;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

public class ValidatorsTest {

  private static <T> void checkNull(Consumer<T> nullConsumer) {
    var e = assertThrows(IllegalArgumentException.class, () -> nullConsumer.accept(null));
    assertTrue(e.getMessage().endsWith("must not be null"));
  }

  private static <T> void assertAllValidate(Validator<T> v, List<T> items) {
    assertFalse(items.isEmpty(), "nothing to check");
    items.forEach(item -> assertSame(item, v.validate(item)));
  }

  private static <T> void assertAllThrow(Validator<T> v, List<T> items) {
    assertFalse(items.isEmpty(), "nothing to check");
    items.forEach(item -> assertThrows(IllegalArgumentException.class, () -> v.validate(item),
        String.valueOf(item)));
  }

  @Test
  public void test_CAN_CLONE_TABLE() {
    Validator<TableId> v = Validators.CAN_CLONE_TABLE;
    checkNull(v::validate);
    assertAllValidate(v, List.of(TableId.of("id1")));
    assertAllThrow(v, List.of(RootTable.ID, MetadataTable.ID));
  }

  @Test
  public void test_EXISTING_NAMESPACE_NAME() {
    Validator<String> v = Validators.EXISTING_NAMESPACE_NAME;
    checkNull(v::validate);
    assertAllValidate(v, List.of(Namespace.DEFAULT.name(), Namespace.ACCUMULO.name(), "normalNs",
        "withNumber2", "has_underscore", "_underscoreStart", StringUtils.repeat("a", 1025)));
    assertAllThrow(v, List.of("has.dot", "has-dash", " hasSpace", ".", "has$dollar"));
  }

  @Test
  public void test_EXISTING_TABLE_NAME() {
    Validator<String> v = Validators.EXISTING_TABLE_NAME;
    checkNull(v::validate);
    assertAllValidate(v,
        List.of(RootTable.NAME, MetadataTable.NAME, "normalTable", "withNumber2", "has_underscore",
            "_underscoreStart", StringUtils.repeat("a", 1025),
            StringUtils.repeat("a", 1025) + "." + StringUtils.repeat("a", 1025)));
    assertAllThrow(v, List.of("has-dash", "has-dash.inNamespace", "has.dash-inTable", " hasSpace",
        ".", "has$dollar", "two.dots.here", ".startsDot"));
  }

  @Test
  public void test_NEW_NAMESPACE_NAME() {
    Validator<String> v = Validators.NEW_NAMESPACE_NAME;
    checkNull(v::validate);
    assertAllValidate(v, List.of(Namespace.DEFAULT.name(), Namespace.ACCUMULO.name(), "normalNs",
        "withNumber2", "has_underscore", "_underscoreStart", StringUtils.repeat("a", 1024)));
    assertAllThrow(v, List.of("has.dot", "has-dash", " hasSpace", ".", "has$dollar",
        StringUtils.repeat("a", 1025)));
  }

  @Test
  public void test_NEW_TABLE_NAME() {
    Validator<String> v = Validators.NEW_TABLE_NAME;
    checkNull(v::validate);
    assertAllValidate(v,
        List.of(RootTable.NAME, MetadataTable.NAME, "normalTable", "withNumber2", "has_underscore",
            "_underscoreStart", StringUtils.repeat("a", 1024),
            StringUtils.repeat("a", 1025) + "." + StringUtils.repeat("a", 1024)));
    assertAllThrow(v,
        List.of("has-dash", "has-dash.inNamespace", "has.dash-inTable", " hasSpace", ".",
            "has$dollar", "two.dots.here", ".startsDot", StringUtils.repeat("a", 1025),
            StringUtils.repeat("a", 1025) + "." + StringUtils.repeat("a", 1025)));
  }

  @Test
  public void test_NOT_BUILTIN_NAMESPACE() {
    Validator<String> v = Validators.NOT_BUILTIN_NAMESPACE;
    checkNull(v::validate);
    assertAllValidate(v, List.of("root", "metadata", " .#!)(*$&^", "  ")); // doesn't validate name
    assertAllThrow(v, List.of(Namespace.DEFAULT.name(), Namespace.ACCUMULO.name()));
  }

  @Test
  public void test_NOT_BUILTIN_TABLE() {
    Validator<String> v = Validators.NOT_BUILTIN_TABLE;
    checkNull(v::validate);
    assertAllValidate(v, List.of("root", "metadata", "user", "ns1.table2"));
    assertAllThrow(v, List.of(RootTable.NAME, MetadataTable.NAME));
  }

  @Test
  public void test_NOT_METADATA_TABLE() {
    Validator<String> v = Validators.NOT_METADATA_TABLE;
    checkNull(v::validate);
    assertAllValidate(v, List.of("root", "metadata", "user", "ns1.table2"));
    assertAllThrow(v, List.of(RootTable.NAME, MetadataTable.NAME));
  }

  @Test
  public void test_NOT_ROOT_TABLE_ID() {
    Validator<TableId> v = Validators.NOT_ROOT_TABLE_ID;
    checkNull(v::validate);
    assertAllValidate(v, List.of(TableId.of(""), MetadataTable.ID, TableId.of(" #0(U!$. ")));
    assertAllThrow(v, List.of(RootTable.ID));
  }

  @Test
  public void test_VALID_TABLE_ID() {
    Validator<TableId> v = Validators.VALID_TABLE_ID;
    checkNull(v::validate);
    assertAllValidate(v, List.of(RootTable.ID, MetadataTable.ID, TableId.of("111"),
        TableId.of("aaaa"), TableId.of("r2d2")));
    assertAllThrow(v, List.of(TableId.of(""), TableId.of("#0(U!$"), TableId.of(" #0(U!$. "),
        TableId.of("."), TableId.of(" "), TableId.of("C3P0")));
  }

  @Test
  public void test_sameNamespaceAs() {
    checkNull(Validators::sameNamespaceAs);
    Validator<String> inDefaultNS = Validators.sameNamespaceAs("tableInDefaultNamespace");
    checkNull(inDefaultNS::validate);
    assertAllValidate(inDefaultNS, List.of("t1"));
    assertAllThrow(inDefaultNS, List.of("accumulo.other", "other.t2", ".", "other.", ".malformed"));

    Validator<String> inOtherNS = Validators.sameNamespaceAs("other.tableInOtherNamespace");
    checkNull(inOtherNS::validate);
    assertAllValidate(inOtherNS, List.of("other.t1", "other.t2"));
    assertAllThrow(inOtherNS, List.of("other.", "other", "else.t3"));
  }

}
