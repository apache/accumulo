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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Initial property encoding that (optionally) uses gzip to compress the property map. The encoding
 * version supported is EncodingVersion.V1_0.
 */
public class GzipPropEncoding implements PropSerdes {

  private final EncodingOptions encodingOpts;

  public GzipPropEncoding(final EncodingOptions encodingOpts) {
    this.encodingOpts = encodingOpts;
  }

  /**
   * Serialize the versioned properties. The version information on the properties is updated if the
   * data is successfully serialized.
   *
   * @param vProps
   *          the versioned properties.
   * @return a byte array with the serialized properties.
   */
  @Override
  public byte[] toBytes(final VersionedProperties vProps) {

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {

      // write header - version id, isCompressed
      encodingOpts.encode(dos);

      // write updated property versioning info (data version, time stamp)
      vProps.getVersionInfo().encode(dos);

      // write the property map keys, values.
      if (encodingOpts.isCompressed()) {
        writeMapCompressed(bos, vProps.getAllProperties());
      } else {
        writeMap(dos, vProps.getAllProperties());
      }

      dos.flush();

      return bos.toByteArray();

    } catch (IOException ex) {
      throw new IllegalStateException("Encountered error serializing properties", ex);
    }
  }

  @Override
  public VersionedProperties fromBytes(final byte[] bytes) {

    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis)) {

      // read header - version and isCompressed.
      EncodingOptions opts = new EncodingOptions(dis);

      // read property versioning info (data version, time stamp)
      VersionInfo info = readVersionInfo(dis);

      // read the property map keys, values
      Map<String,String> aMap;
      if (opts.isCompressed()) {
        aMap = readCompressedMap(bis);
      } else {
        aMap = readMap(dis);
      }

      return new VersionedPropertiesImpl(info, aMap);

    } catch (IOException ex) {
      throw new UncheckedIOException("Encountered error deserializing properties", ex);
    }
  }

  private VersionInfo readVersionInfo(DataInputStream dis) throws IOException {
    return new VersionInfo(dis);
  }

  private Map<String,String> readCompressedMap(ByteArrayInputStream bis) throws IOException {

    try (GZIPInputStream gzipIn = new GZIPInputStream(bis);
        DataInputStream dis = new DataInputStream(gzipIn)) {
      return readMap(dis);
    }
  }

  private Map<String,String> readMap(DataInputStream dis) throws IOException {

    Map<String,String> aMap = new HashMap<>();
    int items = dis.readInt();

    for (int i = 0; i < items; i++) {
      Map.Entry<String,String> e = readKV(dis);
      aMap.put(e.getKey(), e.getValue());
    }
    return aMap;
  }

  private void writeMapCompressed(final ByteArrayOutputStream bos, final Map<String,String> aMap)
      throws IOException {

    try (GZIPOutputStream gzipOut = new GZIPOutputStream(bos);
        DataOutputStream dos = new DataOutputStream(gzipOut)) {

      writeMap(dos, aMap);

      gzipOut.flush();
      gzipOut.finish();

    } catch (IOException ex) {
      throw new IOException("Encountered error compressing properties", ex);
    }
  }

  private void writeMap(final DataOutputStream dos, final Map<String,String> aMap)
      throws IOException {

    dos.writeInt(aMap.size());

    aMap.forEach((k, v) -> writeKV(k, v, dos));

    dos.flush();
  }

  private void writeKV(final String k, final String v, final DataOutputStream dos) {
    try {
      dos.writeUTF(k);
      dos.writeUTF(v);
    } catch (IOException ex) {
      throw new UncheckedIOException(
          String.format("Exception encountered writing props k:'%s', v:'%s", k, v), ex);
    }
  }

  private Map.Entry<String,String> readKV(final DataInputStream dis) throws IOException {
    String k = dis.readUTF();
    String v = dis.readUTF();
    return new AbstractMap.SimpleEntry<>(k, v);
  }

}
