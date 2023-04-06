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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.util.Base64;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class DumpZookeeper implements KeywordExecutable {

  private static ZooReaderWriter zk = null;

  @Override
  public String keyword() {
    return "dump-zoo";
  }

  @Override
  public String description() {
    return "Writes Zookeeper data as human readable or XML to a file.";
  }

  private static class Encoded {
    public String encoding;
    public String value;

    Encoded(String e, String v) {
      encoding = e;
      value = v;
    }
  }

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-r", "-root", "--root"},
        description = "Root ZooKeeper directory to start dump at")
    String root = "/";
    @Parameter(names = {"-x", "-xml", "--xml"},
        description = "Output dump as XML (instead of human readable")
    boolean xml = false;
  }

  @Override
  public void execute(String[] args) throws KeeperException, InterruptedException {
    Opts opts = new Opts();
    opts.parseArgs(DumpZookeeper.class.getName(), args);

    PrintStream out = System.out;
    zk = new ZooReaderWriter(opts.getSiteConfiguration());
    if (opts.xml) {
      writeXml(out, opts.root);
    } else {
      writeHumanReadable(out, opts.root);
    }
  }

  public static void main(String[] args) throws KeeperException, InterruptedException {
    new DumpZookeeper().execute(args);
  }

  private static void writeXml(PrintStream out, String root)
      throws KeeperException, InterruptedException {
    write(out, 0, "<dump root='%s'>", root);
    for (String child : zk.getChildren(root)) {
      if (!child.equals("zookeeper")) {
        childXml(out, root, child, 1);
      }
    }
    write(out, 0, "</dump>");
  }

  private static void childXml(PrintStream out, String root, String child, int indent)
      throws KeeperException, InterruptedException {
    String path = root + "/" + child;
    if (root.endsWith("/")) {
      path = root + child;
    }
    Stat stat = zk.getStatus(path);
    if (stat == null) {
      return;
    }
    String type = "node";
    if (stat.getEphemeralOwner() != 0) {
      type = "ephemeral";
    }
    if (stat.getNumChildren() == 0) {
      if (stat.getDataLength() == 0) {
        write(out, indent, "<%s name='%s'/>", type, child);
      } else {
        Encoded value = value(path);
        write(out, indent, "<%s name='%s' encoding='%s' value='%s'/>", type, child, value.encoding,
            value.value);
      }
    } else {
      if (stat.getDataLength() == 0) {
        write(out, indent, "<%s name='%s'>", type, child);
      } else {
        Encoded value = value(path);
        write(out, indent, "<%s name='%s' encoding='%s' value='%s'>", type, child, value.encoding,
            value.value);
      }
      for (String c : zk.getChildren(path)) {
        childXml(out, path, c, indent + 1);
      }
      write(out, indent, "</node>");
    }
  }

  private static Encoded value(String path) throws KeeperException, InterruptedException {
    byte[] data = zk.getData(path);
    for (byte element : data) {
      // does this look like simple ascii?
      if (element < ' ' || element > '~') {
        return new Encoded("base64", Base64.getEncoder().encodeToString(data));
      }
    }
    return new Encoded(UTF_8.name(), new String(data, UTF_8));
  }

  private static void write(PrintStream out, int indent, String fmt, Object... args) {
    for (int i = 0; i < indent; i++) {
      out.print("  ");
    }
    out.printf(fmt + "%n", args);
  }

  private static void writeHumanReadable(PrintStream out, String root)
      throws KeeperException, InterruptedException {
    write(out, 0, "%s:", root);
    for (String child : zk.getChildren(root)) {
      if (!child.equals("zookeeper")) {
        childHumanReadable(out, root, child, 1);
      }
    }
  }

  private static void childHumanReadable(PrintStream out, String root, String child, int indent)
      throws KeeperException, InterruptedException {
    String path = root + "/" + child;
    if (root.endsWith("/")) {
      path = root + child;
    }
    Stat stat = zk.getStatus(path);
    if (stat == null) {
      return;
    }
    String node = child;
    if (stat.getEphemeralOwner() != 0) {
      node = "*" + child + "*";
    }
    if (stat.getDataLength() == 0) {
      write(out, indent, "%s:", node);
    } else {
      write(out, indent, "%s:  %s", node, value(path).value);
    }
    if (stat.getNumChildren() > 0) {
      for (String c : zk.getChildren(path)) {
        childHumanReadable(out, path, c, indent + 1);
      }
    }
  }
}
