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
package org.apache.accumulo.server.conf.util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// TODO - this is in progress and should not be merged without changes.
// TODO - implement json output (or remove option)
@AutoService(KeywordExecutable.class)
@SuppressFBWarnings(value = "PATH_TRAVERSAL_OUT",
    justification = "app is run in same security context as user providing the filename")
public class ConfigPropertyPrinter implements KeywordExecutable {

  private static final Logger log = LoggerFactory.getLogger(ConfigPropertyPrinter.class);

  public ConfigPropertyPrinter() {}

  public static void main(String[] args) throws Exception {
    new ConfigPropertyPrinter().execute(args);
  }

  @Override
  public String keyword() {
    return "config-property-print";
  }

  @Override
  public String description() {
    return "prints the properties stored in ZooKeeper";
  }

  @Override
  public void execute(String[] args) throws Exception {

    ConfigPropertyPrinter.Opts opts = new ConfigPropertyPrinter.Opts();
    opts.parseArgs(ConfigPropertyPrinter.class.getName(), args);

    ServerContext context = opts.getServerContext();

    print(context, opts.outfile, opts.jsonFmt);

  }

  public void print(final ServerContext context, final String outfile, final boolean jsonFmt) {

    Map<String,String> namespaces;
    Map<String,String> tables;

    AccumuloClient client = Accumulo.newClient().from(context.getProperties()).build();

    ZooReaderWriter zrw = context.getZooReaderWriter();

    try {
      namespaces = client.namespaceOperations().namespaceIdMap();
      tables = client.tableOperations().tableIdMap();
    } catch (AccumuloException | AccumuloSecurityException ex) {
      throw new IllegalStateException("Failed to read namespaces / tables", ex);
    }

    log.info("Namespaces: {}", namespaces);
    log.info("Tables: {}", tables);

    PrintStream origStream = null;

    try {

      OutputStream outStream;

      if (outfile == null || "".equals(outfile)) {
        log.info("No output file, using stdout.");
        origStream = System.out;
        outStream = System.out;
      } else {
        outStream = new FileOutputStream(outfile);
      }

      try (PrintWriter writer =
          new PrintWriter(new BufferedWriter(new OutputStreamWriter(outStream)))) {

        Stat stat = new Stat();
        byte[] bytes;

        try {
          bytes = zrw.getData(PropCacheId.forSystem(context).getPath(), stat);
          VersionedProperties sysProps =
              ZooPropStore.getCodec().fromBytes(stat.getVersion(), bytes);
          printProps(writer, "System", sysProps);
        } catch (KeeperException.NoNodeException nex) {
          // skip on no node.
        }

        for (Map.Entry<String,String> e : namespaces.entrySet()) {
          try {
            bytes = zrw.getData(
                PropCacheId.forNamespace(context, NamespaceId.of(e.getValue())).getPath(), stat);
            VersionedProperties nsProps =
                ZooPropStore.getCodec().fromBytes(stat.getVersion(), bytes);
            printProps(writer, e.getKey(), nsProps);
          } catch (KeeperException.NoNodeException nex) {
            // skip on no node.
          }
        }

        for (Map.Entry<String,String> e : tables.entrySet()) {
          try {
            bytes = zrw.getData(PropCacheId.forTable(context, TableId.of(e.getValue())).getPath(),
                stat);
            VersionedProperties tsProps =
                ZooPropStore.getCodec().fromBytes(stat.getVersion(), bytes);
            printProps(writer, e.getKey(), tsProps);
          } catch (KeeperException.NoNodeException nex) {
            // skip on no node.
          }
        }
      } catch (InterruptedException | KeeperException ex) {
        throw new IllegalStateException("ZooKeeper exception reading properties", ex);
      }
      outStream.close();
      if (origStream != null) {
        System.setOut(origStream);
      }

    } catch (IOException ex) {
      throw new IllegalStateException("Invalid file", ex);
    }

  }

  private void printProps(final PrintWriter writer, final String name,
      final VersionedProperties props) {
    writer.println("************* name: '" + name + "', size: " + props.getProperties().size());
    Map<String,String> sorted = new TreeMap<>(props.getProperties());
    writer.println(name);
    sorted
        .forEach((k, v) -> writer.printf("%s, %s%n", k, Property.isSensitive(v) ? "<hidden>" : v));
  }

  private void printToWriter(PrintWriter writer) {
    writer.println("Printer to a writer");
  }

  static class Opts extends ServerUtilOpts {
    @Parameter(names = {"--outfile"},
        description = "Write the output to a file, if the file exists will not be overwritten.")
    public String outfile = "";

    @Parameter(names = {"-j", "--json"}, description = "format the output in json")
    public boolean jsonFmt = false;
  }

}
