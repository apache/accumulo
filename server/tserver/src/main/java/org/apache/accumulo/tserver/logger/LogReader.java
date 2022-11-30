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
package org.apache.accumulo.tserver.logger;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.NoFileEncrypter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.LogHeaderIncompleteException;
import org.apache.accumulo.tserver.log.RecoveryLogsIterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
public class LogReader implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(LogReader.class);

  static class Opts extends ConfigOpts {
    @Parameter(names = "-e",
        description = "Don't read full log and print only encryption information")
    boolean printOnlyEncryptionInfo = false;
    @Parameter(names = "-r", description = "print only mutations associated with the given row")
    String row;
    @Parameter(names = "-m", description = "limit the number of mutations printed per row")
    int maxMutations = 5;
    @Parameter(names = "-t",
        description = "print only mutations that fall within the given key extent")
    String extent;
    @Parameter(names = "--regex", description = "search for a row that matches the given regex")
    String regexp;
    @Parameter(description = "<logfile> { <logfile> ... }")
    List<String> files = new ArrayList<>();
  }

  /**
   * Dump a Log File to stdout. Will read from HDFS or local file system.
   *
   * @param args - first argument is the file to print
   */
  public static void main(String[] args) throws Exception {
    new LogReader().execute(args);
  }

  @Override
  public String keyword() {
    return "wal-info";
  }

  @Override
  public String description() {
    return "Prints WAL Info";
  }

  @SuppressFBWarnings(value = "DM_EXIT",
      justification = "System.exit is fine here because it's a utility class executed by a main()")
  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("accumulo wal-info", args);
    if (opts.files.isEmpty()) {
      System.err.println("No WAL files were given");
      System.exit(1);
    }

    var siteConfig = opts.getSiteConfiguration();
    ServerContext context = new ServerContext(siteConfig);
    try (VolumeManager fs = context.getVolumeManager()) {
      var walCryptoService = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.WAL,
          siteConfig.getAllCryptoProperties());

      Matcher rowMatcher = null;
      KeyExtent ke = null;
      Text row = null;
      if (opts.row != null) {
        row = new Text(opts.row);
      }
      if (opts.extent != null) {
        String[] sa = opts.extent.split(";");
        ke = new KeyExtent(TableId.of(sa[0]), new Text(sa[1]), new Text(sa[2]));
      }
      if (opts.regexp != null) {
        Pattern pattern = Pattern.compile(opts.regexp);
        rowMatcher = pattern.matcher("");
      }

      Set<Integer> tabletIds = new HashSet<>();

      for (String file : opts.files) {
        Path path = new Path(file);
        LogFileKey key = new LogFileKey();
        LogFileValue value = new LogFileValue();

        // ensure it's a regular non-sorted WAL file, and not a single sorted WAL in RFile format
        if (fs.getFileStatus(path).isFile()) {
          if (file.endsWith(".rf")) {
            log.error("Unable to read from a single RFile. A non-sorted WAL file was expected. "
                + "To read sorted WALs, please pass in a directory containing the sorted recovery logs.");
            continue;
          }

          if (opts.printOnlyEncryptionInfo) {
            try (final FSDataInputStream fsinput = fs.open(path)) {
              printCryptoParams(fsinput, path);
            }
            continue;
          }

          try (final FSDataInputStream fsinput = fs.open(path);
              DataInputStream input = DfsLogger.getDecryptingStream(fsinput, walCryptoService)) {
            while (true) {
              try {
                key.readFields(input);
                value.readFields(input);
              } catch (EOFException ex) {
                break;
              }
              printLogEvent(key, value, row, rowMatcher, ke, tabletIds, opts.maxMutations);
            }
          } catch (LogHeaderIncompleteException e) {
            log.warn("Could not read header for {} . Ignoring...", path);
          } finally {
            log.info("Done reading {}", path);
          }
        } else {
          // read the log entries in a sorted RFile. This has to be a directory that contains the
          // finished file.
          try (var rli = new RecoveryLogsIterator(context, Collections.singletonList(path), null,
              null, false)) {
            while (rli.hasNext()) {
              Entry<LogFileKey,LogFileValue> entry = rli.next();
              printLogEvent(entry.getKey(), entry.getValue(), row, rowMatcher, ke, tabletIds,
                  opts.maxMutations);
            }
          }
        }
      }
    }
  }

  private void printCryptoParams(FSDataInputStream input, Path path) {
    byte[] magic4 = DfsLogger.LOG_FILE_HEADER_V4.getBytes(UTF_8);
    byte[] magic3 = DfsLogger.LOG_FILE_HEADER_V3.getBytes(UTF_8);
    byte[] noCryptoBytes = new NoFileEncrypter().getDecryptionParameters();

    if (magic4.length != magic3.length) {
      throw new AssertionError("Always expect log file headers to be same length : " + magic4.length
          + " != " + magic3.length);
    }

    byte[] magicBuffer = new byte[magic4.length];
    try {
      input.readFully(magicBuffer);
      if (Arrays.equals(magicBuffer, magic4)) {
        byte[] cryptoParams = CryptoUtils.readParams(input);
        if (Arrays.equals(noCryptoBytes, cryptoParams)) {
          System.out.println("No on disk encryption detected.");
        } else {
          System.out.println("Encrypted with Params: "
              + Key.toPrintableString(cryptoParams, 0, cryptoParams.length, cryptoParams.length));
        }
      } else if (Arrays.equals(magicBuffer, magic3)) {
        // Read logs files from Accumulo 1.9 and throw an error if they are encrypted
        String cryptoModuleClassname = input.readUTF();
        if (!cryptoModuleClassname.equals("NullCryptoModule")) {
          throw new IllegalArgumentException(
              "Old encryption modules not supported at this time.  Unsupported module : "
                  + cryptoModuleClassname);
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported write ahead log version " + new String(magicBuffer));
      }
    } catch (EOFException e) {
      log.warn("Could not read header for {} . Ignoring...", path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void printLogEvent(LogFileKey key, LogFileValue value, Text row, Matcher rowMatcher,
      KeyExtent ke, Set<Integer> tabletIds, int maxMutations) {

    if (ke != null) {
      if (key.event == LogEvents.DEFINE_TABLET) {
        if (key.tablet.equals(ke)) {
          tabletIds.add(key.tabletId);
        } else {
          return;
        }
      } else if (!tabletIds.contains(key.tabletId)) {
        return;
      }
    }

    if (row != null || rowMatcher != null) {
      if (key.event == LogEvents.MUTATION || key.event == LogEvents.MANY_MUTATIONS) {
        boolean found = false;
        for (Mutation m : value.mutations) {
          if (row != null && new Text(m.getRow()).equals(row)) {
            found = true;
            break;
          }

          if (rowMatcher != null) {
            rowMatcher.reset(new String(m.getRow(), UTF_8));
            if (rowMatcher.matches()) {
              found = true;
              break;
            }
          }
        }

        if (!found) {
          return;
        }
      } else {
        return;
      }

    }
    System.out.println(key);
    System.out.println(LogFileValue.format(value, maxMutations));
  }
}
