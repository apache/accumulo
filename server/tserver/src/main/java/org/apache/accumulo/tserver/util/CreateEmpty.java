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
package org.apache.accumulo.tserver.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.tserver.log.DfsLogger.LOG_FILE_HEADER_V4;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.file.rfile.compression.NoCompression;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.auto.service.AutoService;

/**
 * Create an empty RFile for use in recovering from data loss where Accumulo still refers internally
 * to a path.
 */
@AutoService(KeywordExecutable.class)
public class CreateEmpty implements KeywordExecutable {
  private static final Logger LOG = LoggerFactory.getLogger(CreateEmpty.class);
  public static final String RF_EXTENSION = ".rf";
  public static final String WAL_EXTENSION = ".wal";

  public static class MatchesValidFileExtension implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value.endsWith(RF_EXTENSION) || value.endsWith(WAL_EXTENSION)) {
        return;
      }
      throw new ParameterException("File must end with either " + RF_EXTENSION + " or "
          + WAL_EXTENSION + " and '" + value + "' does not.");
    }
  }

  public static class IsSupportedCompressionAlgorithm implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      List<String> algorithms = Compression.getSupportedAlgorithms();
      if (!algorithms.contains(value)) {
        throw new ParameterException("Compression codec must be one of " + algorithms);
      }
    }
  }

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-c", "--codec"}, description = "the compression codec to use.",
        validateWith = IsSupportedCompressionAlgorithm.class)
    String codec = new NoCompression().getName();
    @Parameter(
        description = " <path> { <path> ... } Each path given is a URL."
            + " Relative paths are resolved according to the default filesystem defined in"
            + " your Hadoop configuration, which is usually an HDFS instance.",
        required = true, validateWith = MatchesValidFileExtension.class)
    List<String> files = new ArrayList<>();

    public enum OutFileType {
      RF, WAL
    }

    // rfile as default keeps previous behaviour
    @Parameter(names = "--type")
    public OutFileType fileType = OutFileType.RF;

  }

  public static void main(String[] args) throws Exception {
    new CreateEmpty().execute(args);
  }

  @Override
  public String keyword() {
    return "create-empty";
  }

  @Override
  public String description() {
    return "Creates empty RFiles (RF) or empty write-ahead log (WAL) files for emergency recovery";
  }

  @Override
  public void execute(String[] args) throws Exception {

    Opts opts = new Opts();
    opts.parseArgs("accumulo create-empty", args);

    var siteConfig = opts.getSiteConfiguration();
    try (ServerContext context = new ServerContext(siteConfig)) {
      switch (opts.fileType) {
        case RF:
          createEmptyRFile(opts, context);
          break;
        case WAL:
          createEmptyWal(opts, context);
          break;
        default:
          throw new ParameterException("file type must be RF or WAL, received: " + opts.fileType);
      }
    }
  }

  void createEmptyRFile(final Opts opts, final ServerContext context) throws IOException {
    var vm = context.getVolumeManager();

    CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.TABLE);
    CryptoService cryptoService = context.getCryptoFactory().getService(env,
        context.getConfiguration().getAllCryptoProperties());

    for (String filename : opts.files) {
      Path path = new Path(filename);
      checkFileExists(path, vm);
      UnreferencedTabletFile tabletFile =
          UnreferencedTabletFile.of(vm.getFileSystemByPath(path), path);
      LOG.info("Writing to file '{}'", tabletFile);
      FileSKVWriter writer = new RFileOperations().newWriterBuilder()
          .forFile(tabletFile, vm.getFileSystemByPath(path), context.getHadoopConf(), cryptoService)
          .withTableConfiguration(DefaultConfiguration.getInstance()).withCompression(opts.codec)
          .build();
      writer.close();
    }
  }

  void createEmptyWal(Opts opts, ServerContext context) throws IOException {
    final LogFileValue EMPTY = new LogFileValue();

    var vm = context.getVolumeManager();

    for (String filename : opts.files) {
      Path path = new Path(filename);
      checkFileExists(path, vm);
      try (var out = new DataOutputStream(vm.create(path))) {
        LOG.info("Output file: {}", path);

        out.write(LOG_FILE_HEADER_V4.getBytes(UTF_8));

        CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.WAL);
        CryptoService cryptoService = context.getCryptoFactory().getService(env,
            context.getConfiguration().getAllCryptoProperties());

        byte[] cryptoParams = cryptoService.getFileEncrypter(env).getDecryptionParameters();
        CryptoUtils.writeParams(cryptoParams, out);

        LogFileKey key = new LogFileKey();
        key.event = OPEN;
        key.tserverSession = "";

        key.write(out);
        EMPTY.write(out);
      }
    }
  }

  private void checkFileExists(final Path path, final VolumeManager vm) throws IOException {
    if (vm.exists(path)) {
      throw new IllegalArgumentException(path + " exists");
    }
  }
}
