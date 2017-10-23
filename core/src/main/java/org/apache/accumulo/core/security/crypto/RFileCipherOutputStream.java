/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * see the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.security.crypto;

import java.io.IOException;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

/**
 *
 * This class extends {@link CipherOutputStream} to include a way to track the number of bytes that have
 * been encrypted by the stream. The write method also includes a mechanism to stop writing and
 * throw an exception if exceeding a maximum number of bytes is attempted.
 *
 */
public class RFileCipherOutputStream extends CipherOutputStream {

  // This is the maximum size encrypted stream that can be written. Attempting to write anything larger
  // will cause an exception. Given that each block in an rfile is encrypted separately, and blocks
  // should be written such that a block cannot ever reach 16GiB, this is believed to be a safe number.
  // If this does cause an exception, it is an issue best addressed elsewhere.
  private final long maxOutputSize = 1L << 34; //16GiB

  // the cipher engine to use to process stream data
  private Cipher cipher;

  // the underlying output stream
  private OutputStream output;

  // the buffer holding data ready to be written out
  private byte[] obuffer;

  // The total number of bytes that have been written out
  private long count = 0;

  /**
   *
   * Constructs a RFileCipherOutputStream
   *
   * @param os
   *          the OutputStream object
   * @param c
   *          an initialized Cipher object
   */
  public RFileCipherOutputStream(OutputStream os, Cipher c) {
    super(os, c);
    output = os;
    cipher = c;
  }

  /**
   * Override of CipherOutputStream's write to count the number of bytes that have been encrypted.
   * This method now throws an exception if an attempt to write bytes beyond a maximum is made.
   *
   * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off</code> to this output stream.
   *
   * @param b
   *          the data.
   * @param off
   *          the start offset in the data.
   * @param len
   *          the number of bytes to write.
   * @exception IOException
   *              if an I/O error occurs.
   * @since JCE1.2
   */
  @Override
  public void write(byte b[], int off, int len) throws IOException {
    count += len;
    if (count > maxOutputSize) {
      throw new IOException("Attempt to write " + count + " bytes was made. A maximum of " + maxOutputSize + " is allowed for an encryption stream.");
    }
    obuffer = cipher.update(b, off, len);
    if (obuffer != null) {
      output.write(obuffer);
      obuffer = null;
    }
  }
}
