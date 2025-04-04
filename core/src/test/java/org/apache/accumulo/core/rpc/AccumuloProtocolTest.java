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
package org.apache.accumulo.core.rpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

public class AccumuloProtocolTest {

  private static final int VALID_MAGIC_NUMBER =
      AccumuloProtocolFactory.AccumuloProtocol.MAGIC_NUMBER;
  private static final int INVALID_MAGIC_NUMBER = 0x12345678;
  private static final byte VALID_VERSION =
      AccumuloProtocolFactory.AccumuloProtocol.PROTOCOL_VERSION;
  private static final byte INVALID_VERSION = 99;

  /**
   * Test that a valid header does not throw an exception
   */
  @Test
  public void testValidHeader() throws TException {
    try (TMemoryBuffer transport = new TMemoryBuffer(100)) {

      TCompactProtocol protocol = new TCompactProtocol(transport);
      protocol.writeI32(VALID_MAGIC_NUMBER);
      protocol.writeByte(VALID_VERSION);
      protocol.writeBool(false);

      var serverProtocol = (AccumuloProtocolFactory.AccumuloProtocol) AccumuloProtocolFactory
          .serverFactory().getProtocol(transport);

      assertDoesNotThrow(serverProtocol::validateHeader);

      assertEquals(0, transport.read(new byte[10], 0, 10), "Expected all data to be consumed");
    }
  }

  /**
   * Test that an invalid magic number throws an exception
   */
  @Test
  public void testInvalidMagicNumber() throws TException {
    try (TMemoryBuffer transport = new TMemoryBuffer(100)) {

      TCompactProtocol protocol = new TCompactProtocol(transport);
      protocol.writeI32(INVALID_MAGIC_NUMBER);
      protocol.writeByte(VALID_VERSION);
      protocol.writeBool(false);

      AccumuloProtocolFactory.AccumuloProtocol serverProtocol =
          (AccumuloProtocolFactory.AccumuloProtocol) AccumuloProtocolFactory.serverFactory()
              .getProtocol(transport);

      TException exception = assertThrows(TException.class, serverProtocol::validateHeader);
      assertTrue(exception.getMessage().contains("magic number mismatch"),
          "Expected bad magic number msg. Got: " + exception.getMessage());
    }
  }

  /**
   * Test that an incompatible version number throws an exception
   */
  @Test
  public void testIncompatibleVersion() throws TException {
    try (TMemoryBuffer transport = new TMemoryBuffer(100)) {

      TCompactProtocol protocol = new TCompactProtocol(transport);
      protocol.writeI32(VALID_MAGIC_NUMBER);
      protocol.writeByte(INVALID_VERSION);
      protocol.writeBool(false);

      AccumuloProtocolFactory.AccumuloProtocol serverProtocol =
          (AccumuloProtocolFactory.AccumuloProtocol) AccumuloProtocolFactory.serverFactory()
              .getProtocol(transport);

      TException exception = assertThrows(TException.class, serverProtocol::validateHeader);
      assertTrue(exception.getMessage().contains("Incompatible protocol version"),
          "Expected incompatible version msg. Got: " + exception.getMessage());
    }
  }

  /**
   * Test that an incomplete protocol header throws an exception
   */
  @Test
  public void testIncompleteHeader() throws TException {
    try (TMemoryBuffer transport = new TMemoryBuffer(100)) {
      // write incomplete header. just magic number
      TCompactProtocol protocol = new TCompactProtocol(transport);
      protocol.writeI32(VALID_MAGIC_NUMBER);

      AccumuloProtocolFactory.AccumuloProtocol serverProtocol =
          (AccumuloProtocolFactory.AccumuloProtocol) AccumuloProtocolFactory.serverFactory()
              .getProtocol(transport);

      assertThrows(TTransportException.class, serverProtocol::validateHeader);
    }
  }
}
