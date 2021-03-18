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
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.RFile.Writer;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.NoCryptoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

/**
 * Split an RFile into large and small key/value files.
 */
public class SplitLarge {

  static class Opts extends Help {
    @Parameter(names = "-m",
        description = "the maximum size of the key/value pair to shunt to the small file")
    long maxSize = 10 * 1024 * 1024;
    @Parameter(names = "-crypto", description = "the class to perform encryption/decryption")
    String encryptClass = Property.TABLE_CRYPTO_ENCRYPT_SERVICE.getDefaultValue();
    @Parameter(description = "<file.rf> { <file.rf> ... }")
    List<String> files = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Opts opts = new Opts();
    opts.parseArgs(SplitLarge.class.getName(), args);

    for (String file : opts.files) {
      var aconf = new ConfigurationCopy(DefaultConfiguration.getInstance());
      aconf.set(Property.TABLE_CRYPTO_DECRYPT_SERVICES.getKey(), opts.encryptClass);
      CryptoService cs = ConfigurationTypeHelper.getClassInstance(null, opts.encryptClass,
          CryptoService.class, new NoCryptoService());
      var encrypt = cs.getEncrypter();
      Path path = new Path(file);
      CachableBuilder cb = new CachableBuilder().fsPath(fs, path).conf(conf).decrypt(
          CryptoServiceFactory.getDecrypters(aconf, CryptoServiceFactory.ClassloaderType.JAVA));
      try (Reader iter = new RFile.Reader(cb)) {

        if (!file.endsWith(".rf")) {
          throw new IllegalArgumentException("File must end with .rf");
        }
        String smallName = file.substring(0, file.length() - 3) + "_small.rf";
        String largeName = file.substring(0, file.length() - 3) + "_large.rf";

        int blockSize = (int) aconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE);
        try (
            Writer small = new RFile.Writer(
                new BCFile.Writer(fs.create(new Path(smallName)), null, "gz", conf, encrypt),
                blockSize);
            Writer large = new RFile.Writer(
                new BCFile.Writer(fs.create(new Path(largeName)), null, "gz", conf, encrypt),
                blockSize)) {
          small.startDefaultLocalityGroup();
          large.startDefaultLocalityGroup();

          iter.seek(new Range(), new ArrayList<>(), false);
          while (iter.hasTop()) {
            Key key = iter.getTopKey();
            Value value = iter.getTopValue();
            if (key.getSize() + value.getSize() < opts.maxSize) {
              small.append(key, value);
            } else {
              large.append(key, value);
            }
            iter.next();
          }

        }
      }
    }
  }

}
