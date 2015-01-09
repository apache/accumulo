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
package org.apache.accumulo.server.util;

import static com.google.common.base.Charsets.UTF_8;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.beust.jcommander.Parameter;

public class DumpZookeeper {

  static IZooReaderWriter zk = null;

  private static final Logger log = Logger.getLogger(DumpZookeeper.class);

  private static class Encoded {
    public String encoding;
    public String value;

    Encoded(String e, String v) {
      encoding = e;
      value = v;
    }
  }

  static class Opts extends Help {
    @Parameter(names = "--root", description = "the root of the znode tree to dump")
    String root = "/";
  }

  public static void main(String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(DumpZookeeper.class.getName(), args);

    Logger.getRootLogger().setLevel(Level.WARN);
    PrintStream out = System.out;
    try {
      zk = ZooReaderWriter.getInstance();

      write(out, 0, "<dump root='%s'>", opts.root);
      for (String child : zk.getChildren(opts.root, null))
        if (!child.equals("zookeeper"))
          dump(out, opts.root, child, 1);
      write(out, 0, "</dump>");
    } catch (Exception ex) {
      log.error(ex, ex);
    }
  }

  private static void dump(PrintStream out, String root, String child, int indent) throws KeeperException, InterruptedException, UnsupportedEncodingException {
    String path = root + "/" + child;
    if (root.endsWith("/"))
      path = root + child;
    Stat stat = zk.getStatus(path);
    if (stat == null)
      return;
    String type = "node";
    if (stat.getEphemeralOwner() != 0) {
      type = "ephemeral";
    }
    if (stat.getNumChildren() == 0) {
      if (stat.getDataLength() == 0) {
        write(out, indent, "<%s name='%s'/>", type, child);
      } else {
        Encoded value = value(path);
        write(out, indent, "<%s name='%s' encoding='%s' value='%s'/>", type, child, value.encoding, value.value);
      }
    } else {
      if (stat.getDataLength() == 0) {
        write(out, indent, "<%s name='%s'>", type, child);
      } else {
        Encoded value = value(path);
        write(out, indent, "<%s name='%s' encoding='%s' value='%s'>", type, child, value.encoding, value.value);
      }
      for (String c : zk.getChildren(path, null)) {
        dump(out, path, c, indent + 1);
      }
      write(out, indent, "</node>");
    }
  }

  private static Encoded value(String path) throws KeeperException, InterruptedException, UnsupportedEncodingException {
    byte[] data = zk.getData(path, null);
    for (int i = 0; i < data.length; i++) {
      // does this look like simple ascii?
      if (data[i] < ' ' || data[i] > '~')
        return new Encoded("base64", Base64.encodeBase64String(data));
    }
    return new Encoded(UTF_8.name(), new String(data, UTF_8));
  }

  private static void write(PrintStream out, int indent, String fmt, Object... args) {
    for (int i = 0; i < indent; i++)
      out.print(" ");
    out.println(String.format(fmt, args));
  }
}
