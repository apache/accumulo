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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Base64;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.zookeeper.KeeperException;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RestoreZookeeper {

  private static class Restore extends DefaultHandler {
    ZooReaderWriter zk = null;
    Stack<String> cwd = new Stack<>();
    boolean overwrite = false;

    Restore(ZooReaderWriter zk, boolean overwrite) {
      this.zk = zk;
      this.overwrite = overwrite;
    }

    @Override
    public void startElement(String uri, String localName, String name, Attributes attributes) {
      if ("node".equals(name)) {
        String child = attributes.getValue("name");
        if (child == null) {
          throw new RuntimeException("name attribute not set");
        }
        String encoding = attributes.getValue("encoding");
        String value = attributes.getValue("value");
        if (value == null) {
          value = "";
        }
        String path = cwd.lastElement() + "/" + child;
        create(path, value, encoding);
        cwd.push(path);
      } else if ("dump".equals(name)) {
        String root = attributes.getValue("root");
        if (root.equals("/")) {
          cwd.push("");
        } else {
          cwd.push(root);
        }
        create(root, "", UTF_8.name());
      } else if ("ephemeral".equals(name)) {
        cwd.push("");
      }
    }

    @Override
    public void endElement(String uri, String localName, String name) {
      cwd.pop();
    }

    // assume UTF-8 if not "base64"
    private void create(String path, String value, String encoding) {
      byte[] data = value.getBytes(UTF_8);
      if ("base64".equals(encoding)) {
        data = Base64.getDecoder().decode(data);
      }
      try {
        try {
          zk.putPersistentData(path, data,
              overwrite ? NodeExistsPolicy.OVERWRITE : NodeExistsPolicy.FAIL);
        } catch (KeeperException e) {
          if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
            throw new RuntimeException(path + " exists.  Remove it first.");
          }
          throw e;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "code runs in same security context as user who provided input")
  public static void execute(final AccumuloConfiguration conf, final String file,
      final boolean overwrite) throws Exception {
    var zoo = new ZooReaderWriter(conf);

    InputStream in = System.in;
    if (file != null) {
      in = new FileInputStream(file);
    }

    SAXParserFactory factory = SAXParserFactory.newInstance();
    // Prevent external entities by failing on any doctypes. We don't expect any doctypes, so this
    // is a simple switch to remove any chance of external entities causing problems.
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    SAXParser parser = factory.newSAXParser();
    parser.parse(in, new Restore(zoo, overwrite));
    in.close();
  }

}
