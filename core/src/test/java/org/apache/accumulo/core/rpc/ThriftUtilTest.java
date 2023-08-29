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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.accumulo.core.rpc.ThriftUtil.AccumuloTFramedTransportFactory;
import org.apache.thrift.transport.TByteBuffer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Test;

public class ThriftUtilTest {

  public static final int FRAME_HDR_SIZE = 4;
  public static final int MB1 = 100 * 1024 * 1024;
  public static final int MB10 = 100 * 1024 * 1024;
  public static final int MB100 = 100 * 1024 * 1024;
  public static final int GB = 1 * 1024 * 1024 * 1024;

  @Test
  public void testDefaultTFramedTransportFactory() throws TTransportException {

    TByteBuffer underlyingTransport = new TByteBuffer(ByteBuffer.allocate(1024));

    TFramedTransport.Factory factory = new TFramedTransport.Factory(GB);
    TTransport framedTransport = factory.getTransport(underlyingTransport);

    assertEquals(framedTransport.getConfiguration().getMaxFrameSize(), GB);
    assertEquals(framedTransport.getConfiguration().getMaxMessageSize(), MB100);
  }

  @Test
  public void testAccumuloTFramedTransportFactory() throws TTransportException {
    TByteBuffer underlyingTransport = new TByteBuffer(ByteBuffer.allocate(1024));

    AccumuloTFramedTransportFactory factory = new AccumuloTFramedTransportFactory(GB);
    TTransport framedTransport = factory.getTransport(underlyingTransport);

    assertEquals(framedTransport.getConfiguration().getMaxFrameSize(), GB);
    assertEquals(framedTransport.getConfiguration().getMaxMessageSize(), GB);
  }

  @Test
  public void testMessageSizeReadWriteSuccess() throws Exception {
    TByteBuffer underlyingTransport = new TByteBuffer(ByteBuffer.allocate(MB10));
    AccumuloTFramedTransportFactory factory = new AccumuloTFramedTransportFactory(MB1);
    TTransport framedTransport = factory.getTransport(underlyingTransport);

    // Write more than 1MB to the TByteBuffer, this does not fail.
    byte[] writeBuf = new byte[MB1 - FRAME_HDR_SIZE];
    Arrays.fill(writeBuf, (byte) 1);
    framedTransport.write(writeBuf);
    framedTransport.flush();

    assertEquals(MB1, underlyingTransport.getByteBuffer().position());
    underlyingTransport.flip();
    assertEquals(0, underlyingTransport.getByteBuffer().position());
    assertEquals(MB1, underlyingTransport.getByteBuffer().limit());

    // Try to read more than 1MB from the TByteBuffer, this fails
    byte[] readBuf = new byte[MB1];
    framedTransport.read(readBuf, 0, MB1);
  }

  @Test
  public void testMessageSizeWriteFailure() throws Exception {
    TByteBuffer underlyingTransport = new TByteBuffer(ByteBuffer.allocate(MB10));
    AccumuloTFramedTransportFactory factory = new AccumuloTFramedTransportFactory(MB1);
    TTransport framedTransport = factory.getTransport(underlyingTransport);

    // Unable to write more than 1MB to the TByteBuffer
    byte[] writeBuf = new byte[MB1 + 100];
    Arrays.fill(writeBuf, (byte) 1);
    framedTransport.write(writeBuf);
    assertThrows(TTransportException.class, () -> framedTransport.flush());
  }
}
