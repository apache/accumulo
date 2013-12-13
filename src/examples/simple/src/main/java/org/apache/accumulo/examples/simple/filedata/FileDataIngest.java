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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.examples.simple.filedata;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * Takes a list of files and archives them into Accumulo keyed on hashes of the files. See docs/examples/README.filedata for instructions.
 */
public class FileDataIngest {
  public static final Text CHUNK_CF = new Text("~chunk");
  public static final Text REFS_CF = new Text("refs");
  public static final String REFS_ORIG_FILE = "name";
  public static final String REFS_FILE_EXT = "filext";
  public static final ByteSequence CHUNK_CF_BS = new ArrayByteSequence(CHUNK_CF.getBytes(), 0, CHUNK_CF.getLength());
  public static final ByteSequence REFS_CF_BS = new ArrayByteSequence(REFS_CF.getBytes(), 0, REFS_CF.getLength());

  int chunkSize;
  byte[] chunkSizeBytes;
  byte[] buf;
  MessageDigest md5digest;
  ColumnVisibility cv;

  public FileDataIngest(int chunkSize, ColumnVisibility colvis) {
    this.chunkSize = chunkSize;
    chunkSizeBytes = intToBytes(chunkSize);
    buf = new byte[chunkSize];
    try {
      md5digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    cv = colvis;
  }

  public String insertFileData(String filename, BatchWriter bw) throws MutationsRejectedException, IOException {
    if (chunkSize == 0)
      return "";
    md5digest.reset();
    String uid = hexString(md5digest.digest(filename.getBytes()));

    // read through file once, calculating hashes
    md5digest.reset();
    InputStream fis = new FileInputStream(filename);
    int numRead = fis.read(buf);
    while (numRead >= 0) {
      if (numRead > 0) {
        md5digest.update(buf, 0, numRead);
      }
      numRead = fis.read(buf);
    }
    fis.close();

    String hash = hexString(md5digest.digest());
    Text row = new Text(hash);

    // write info to accumulo
    Mutation m = new Mutation(row);
    m.put(REFS_CF, KeyUtil.buildNullSepText(uid, REFS_ORIG_FILE), cv, new Value(filename.getBytes()));
    String fext = getExt(filename);
    if (fext != null)
      m.put(REFS_CF, KeyUtil.buildNullSepText(uid, REFS_FILE_EXT), cv, new Value(fext.getBytes()));
    bw.addMutation(m);

    // read through file again, writing chunks to accumulo
    int chunkCount = 0;
    try {
      fis = new FileInputStream(filename);
      numRead = fis.read(buf);
      while (numRead >= 0) {
        while (numRead < buf.length) {
          int moreRead = fis.read(buf, numRead, buf.length - numRead);
          if (moreRead > 0)
            numRead += moreRead;
          else if (moreRead < 0)
            break;
        }
        m = new Mutation(row);
        Text chunkCQ = new Text(chunkSizeBytes);
        chunkCQ.append(intToBytes(chunkCount), 0, 4);
        m.put(CHUNK_CF, chunkCQ, cv, new Value(buf, 0, numRead));
        bw.addMutation(m);
        if (chunkCount == Integer.MAX_VALUE)
          throw new RuntimeException("too many chunks for file " + filename + ", try raising chunk size");
        chunkCount++;
        numRead = fis.read(buf);
      }
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
    m = new Mutation(row);
    Text chunkCQ = new Text(chunkSizeBytes);
    chunkCQ.append(intToBytes(chunkCount), 0, 4);
    m.put(new Text(CHUNK_CF), chunkCQ, cv, new Value(new byte[0]));
    bw.addMutation(m);
    return hash;
  }

  public static int bytesToInt(byte[] b, int offset) {
    if (b.length <= offset + 3)
      throw new NumberFormatException("couldn't pull integer from bytes at offset " + offset);
    int i = (((b[offset] & 255) << 24) + ((b[offset + 1] & 255) << 16) + ((b[offset + 2] & 255) << 8) + ((b[offset + 3] & 255) << 0));
    return i;
  }

  public static byte[] intToBytes(int l) {
    byte[] b = new byte[4];
    b[0] = (byte) (l >>> 24);
    b[1] = (byte) (l >>> 16);
    b[2] = (byte) (l >>> 8);
    b[3] = (byte) (l >>> 0);
    return b;
  }

  private static String getExt(String filename) {
    if (filename.indexOf(".") == -1)
      return null;
    return filename.substring(filename.lastIndexOf(".") + 1);
  }

  public String hexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 8) {
      System.out.println("usage: " + FileDataIngest.class.getSimpleName()
          + " <instance> <zoo> <user> <pass> <data table> <visibility> <data chunk size> <file>{ <file>}");
      System.exit(1);
    }

    String instance = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    String pass = args[3];
    String dataTable = args[4];
    ColumnVisibility colvis = new ColumnVisibility(args[5]);
    int chunkSize = Integer.parseInt(args[6]);

    Connector conn = new ZooKeeperInstance(instance, zooKeepers).getConnector(user, pass.getBytes());
    if (!conn.tableOperations().exists(dataTable)) {
      conn.tableOperations().create(dataTable);
      conn.tableOperations().attachIterator(dataTable, new IteratorSetting(1, ChunkCombiner.class));
    }
    BatchWriter bw = conn.createBatchWriter(dataTable, 50000000, 300000l, 4);
    FileDataIngest fdi = new FileDataIngest(chunkSize, colvis);
    for (int i = 7; i < args.length; i++) {
      fdi.insertFileData(args[i], bw);
    }
    bw.close();
  }
}
