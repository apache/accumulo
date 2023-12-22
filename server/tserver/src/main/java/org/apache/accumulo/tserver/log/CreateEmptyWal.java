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
package org.apache.accumulo.tserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.tserver.log.DfsLogger.LOG_FILE_HEADER_V4;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.spi.crypto.NoFileEncrypter;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class CreateEmptyWal implements KeywordExecutable {
  private static final LogFileValue EMPTY = new LogFileValue();

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "file output path provided by an admin")
  // public to allow jcommander access
  public static class DirValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      Path path = Paths.get(value);
      if (!Files.exists(path)) {
        throw new ParameterException("Directory " + path.toAbsolutePath() + " does not exist");
      }
      if (!Files.isDirectory(path)) {
        throw new ParameterException(path.toAbsolutePath() + " is not a directory");
      }
    }
  }

  static class Opts extends Help {
    @Parameter(names = {"-d", "--dir"},
        description = " <dir> the output directory, output will be [dir]/empty.wal",
        required = true, validateWith = DirValidator.class)
    String dirName;
  }

  @Override
  public String keyword() {
    return "create-empty-wal";
  }

  @Override
  public String description() {
    return "creates an empty wal file in the directory specified";
  }

  @Override
  public void execute(String[] args) throws Exception {

    Opts opts = new Opts();
    opts.parseArgs("accumulo create-empty-wal", args);

    var path = Path.of(opts.dirName + "/empty.wal");

    System.out.println("Output file: " + path.toAbsolutePath());

    try (var out = new DataOutputStream(Files.newOutputStream(path))) {
      out.write(LOG_FILE_HEADER_V4.getBytes(UTF_8));
      byte[] decryptionParams = new NoFileEncrypter().getDecryptionParameters();
      CryptoUtils.writeParams(decryptionParams, out);
      LogFileKey key = new LogFileKey();
      key.event = OPEN;
      key.tserverSession = "";

      key.write(out);
      EMPTY.write(out);
    }
  }
}
