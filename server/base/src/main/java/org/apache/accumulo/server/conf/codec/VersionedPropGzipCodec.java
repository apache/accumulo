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
package org.apache.accumulo.server.conf.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Initial property encoding that (optionally) uses gzip to compress the property map. The encoding
 * version supported is EncodingVersion.V1_0.
 */
public class VersionedPropGzipCodec extends VersionedPropCodec {

  private VersionedPropGzipCodec(final EncodingOptions encodingOpts) {
    super(encodingOpts);
  }

  public static VersionedPropCodec codec(final boolean compress) {
    return new VersionedPropGzipCodec(EncodingOptions.V1_0(compress));
  }

  @Override
  void encodePayload(final OutputStream out, final VersionedProperties vProps,
      final EncodingOptions encodingOpts) throws IOException {

    Map<String,String> props = vProps.asMap();

    if (getEncodingOpts().isCompressed()) {
      // Write the property map to the output stream, compressing the output using GZip
      try (GZIPOutputStream gzipOut = new GZIPOutputStream(out);
          DataOutputStream zdos = new DataOutputStream(gzipOut)) {

        writeMapAsUTF(zdos, props);

        // finalize compression
        gzipOut.flush();
        gzipOut.finish();
      }
    } else {
      try (DataOutputStream dos = new DataOutputStream(out)) {
        writeMapAsUTF(dos, props);
      }
    }

  }

  @Override
  boolean checkCanDecodeVersion(final EncodingOptions encodingOpts) {
    return encodingOpts.getEncodingVersion() == EncodingOptions.EncodingVersion_1_0;
  }

  @Override
  Map<String,String> decodePayload(final InputStream inStream, final EncodingOptions encodingOpts)
      throws IOException {
    // read the property map keys, values
    Map<String,String> aMap;
    if (encodingOpts.isCompressed()) {
      // Read and uncompress an input stream compressed with GZip
      try (GZIPInputStream gzipIn = new GZIPInputStream(inStream);
          DataInputStream zdis = new DataInputStream(gzipIn)) {
        aMap = readMapAsUTF(zdis);
      }
    } else {
      try (DataInputStream dis = new DataInputStream(inStream)) {
        aMap = readMapAsUTF(dis);
      }
    }
    return aMap;
  }
}
