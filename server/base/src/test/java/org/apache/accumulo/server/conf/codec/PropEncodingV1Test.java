/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropEncodingV1Test {

  private static final Logger log = LoggerFactory.getLogger(PropEncodingV1Test.class);

  @Test
  public void defaultConstructor() {
    PropEncoding props = new PropEncodingV1();

    // on first write, the znode version expected should be 0.
    // expected should never be -1. That would just overwrite any znode
    assertEquals(0, props.getExpectedVersion());

    byte[] bytes = props.toBytes();
    PropEncodingV1 decoded = new PropEncodingV1(bytes);

    // initial version in ZooKeeper expected to be 0.
    assertEquals(0, decoded.getDataVersion());
    assertEquals(0, props.getExpectedVersion());

  }

  @Test
  public void propValidation() {
    Map<String,String> propMap = new HashMap<>();
    propMap.put("k1", "v1");
    propMap.put("k2", "v2");

    PropEncoding props = new PropEncodingV1();
    props.addProperties(propMap);

    assertEquals(propMap, props.getAllProperties());

    props.addProperty("k3", "v3");
    assertEquals(3, props.getAllProperties().size());

    props.removeProperty("k2");

    assertEquals(2, props.getAllProperties().size());
    assertEquals("v1", props.getProperty("k1"));
    assertNull("v1", props.getProperty("k2"));
    assertEquals("v3", props.getProperty("k3"));

  }

  @Test
  public void compressedEncodeTest() {

    PropEncoding props = new PropEncodingV1(1, true, Instant.now());
    fillMap(props);

    Map<String,String> propValues = props.getAllProperties();

    int ver = props.getDataVersion();

    var createdTs = props.getTimestamp();

    byte[] bytes = props.toBytes();

    // timestamp updated on serialization
    assertNotEquals(createdTs, props.getTimestamp());
    assertEquals(props.getDataVersion(), ver + 1);

    // expected should be "previous" value that would the node version in ZooKeeper
    assertEquals(ver, props.getExpectedVersion());

    log.debug("compressed encoded length: {}", bytes.length);
    PropEncodingV1 decoded = new PropEncodingV1(bytes);

    log.info("Decoded:\n{}", decoded.print(true));

    assertEquals(propValues, decoded.getAllProperties());
  }

  @Test
  public void uncompressedEncodeTest() {

    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    fillMap(props);

    byte[] bytes = props.toBytes();

    log.debug("uncompressed encoded length: {}", bytes.length);

    PropEncodingV1 decoded = new PropEncodingV1(bytes);

    log.info("Decoded:\n{}", decoded.print(true));

  }

  @Test
  public void acceptNullProps() {
    PropEncoding props = new PropEncodingV1(1, false, Instant.now());
    props.addProperties(null);
  }

  private void fillMap(final PropEncoding props) {
    props.addProperty("key1", "value1");
    props.addProperty("key2", "value2");
    props.addProperty("key3", "value3");
    props.addProperty("key4", "value4");
  }
}
