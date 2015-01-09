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
package org.apache.accumulo.examples.simple.dirlist;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

/**
 * Provides utility methods for getting the info for a file, listing the contents of a directory, and performing single wild card searches on file or directory
 * names. See docs/examples/README.dirlist for instructions.
 */
public class QueryUtil {
  private Connector conn = null;
  private String tableName;
  private Authorizations auths;
  public static final Text DIR_COLF = new Text("dir");
  public static final Text FORWARD_PREFIX = new Text("f");
  public static final Text REVERSE_PREFIX = new Text("r");
  public static final Text INDEX_COLF = new Text("i");
  public static final Text COUNTS_COLQ = new Text("counts");

  public QueryUtil(Opts opts) throws AccumuloException, AccumuloSecurityException {
    conn = opts.getConnector();
    this.tableName = opts.tableName;
    this.auths = opts.auths;
  }

  /**
   * Calculates the depth of a path, i.e. the number of forward slashes in the path name.
   *
   * @param path
   *          the full path of a file or directory
   * @return the depth of the path
   */
  public static int getDepth(String path) {
    int numSlashes = 0;
    int index = -1;
    while ((index = path.indexOf("/", index + 1)) >= 0)
      numSlashes++;
    return numSlashes;
  }

  /**
   * Given a path, construct an accumulo row prepended with the path's depth for the directory table.
   *
   * @param path
   *          the full path of a file or directory
   * @return the accumulo row associated with this path
   */
  public static Text getRow(String path) {
    Text row = new Text(String.format("%03d", getDepth(path)));
    row.append(path.getBytes(), 0, path.length());
    return row;
  }

  /**
   * Given a path, construct an accumulo row prepended with the {@link #FORWARD_PREFIX} for the index table.
   *
   * @param path
   *          the full path of a file or directory
   * @return the accumulo row associated with this path
   */
  public static Text getForwardIndex(String path) {
    String part = path.substring(path.lastIndexOf("/") + 1);
    if (part.length() == 0)
      return null;
    Text row = new Text(FORWARD_PREFIX);
    row.append(part.getBytes(), 0, part.length());
    return row;
  }

  /**
   * Given a path, construct an accumulo row prepended with the {@link #REVERSE_PREFIX} with the path reversed for the index table.
   *
   * @param path
   *          the full path of a file or directory
   * @return the accumulo row associated with this path
   */
  public static Text getReverseIndex(String path) {
    String part = path.substring(path.lastIndexOf("/") + 1);
    if (part.length() == 0)
      return null;
    byte[] rev = new byte[part.length()];
    int i = part.length() - 1;
    for (byte b : part.getBytes())
      rev[i--] = b;
    Text row = new Text(REVERSE_PREFIX);
    row.append(rev, 0, rev.length);
    return row;
  }

  /**
   * Returns either the {@link #DIR_COLF} or a decoded string version of the colf.
   *
   * @param colf
   *          the column family
   */
  public static String getType(Text colf) {
    if (colf.equals(DIR_COLF))
      return colf.toString() + ":";
    return Long.toString(Ingest.encoder.decode(colf.getBytes())) + ":";
  }

  /**
   * Scans over the directory table and pulls out stat information about a path.
   *
   * @param path
   *          the full path of a file or directory
   */
  public Map<String,String> getData(String path) throws TableNotFoundException {
    if (path.endsWith("/"))
      path = path.substring(0, path.length() - 1);
    Scanner scanner = conn.createScanner(tableName, auths);
    scanner.setRange(new Range(getRow(path)));
    Map<String,String> data = new TreeMap<String,String>();
    for (Entry<Key,Value> e : scanner) {
      String type = getType(e.getKey().getColumnFamily());
      data.put("fullname", e.getKey().getRow().toString().substring(3));
      data.put(type + e.getKey().getColumnQualifier().toString() + ":" + e.getKey().getColumnVisibility().toString(), new String(e.getValue().get()));
    }
    return data;
  }

  /**
   * Uses the directory table to list the contents of a directory.
   *
   * @param path
   *          the full path of a directory
   */
  public Map<String,Map<String,String>> getDirList(String path) throws TableNotFoundException {
    if (!path.endsWith("/"))
      path = path + "/";
    Map<String,Map<String,String>> fim = new TreeMap<String,Map<String,String>>();
    Scanner scanner = conn.createScanner(tableName, auths);
    scanner.setRange(Range.prefix(getRow(path)));
    for (Entry<Key,Value> e : scanner) {
      String name = e.getKey().getRow().toString();
      name = name.substring(name.lastIndexOf("/") + 1);
      String type = getType(e.getKey().getColumnFamily());
      if (!fim.containsKey(name)) {
        fim.put(name, new TreeMap<String,String>());
        fim.get(name).put("fullname", e.getKey().getRow().toString().substring(3));
      }
      fim.get(name).put(type + e.getKey().getColumnQualifier().toString() + ":" + e.getKey().getColumnVisibility().toString(), new String(e.getValue().get()));
    }
    return fim;
  }

  /**
   * Scans over the index table for files or directories with a given name.
   *
   * @param term
   *          the name a file or directory to search for
   */
  public Iterable<Entry<Key,Value>> exactTermSearch(String term) throws Exception {
    System.out.println("executing exactTermSearch for " + term);
    Scanner scanner = conn.createScanner(tableName, auths);
    scanner.setRange(new Range(getForwardIndex(term)));
    return scanner;
  }

  /**
   * Scans over the index table for files or directories with a given name, prefix, or suffix (indicated by a wildcard '*' at the beginning or end of the term.
   *
   * @param exp
   *          the name a file or directory to search for with an optional wildcard '*' at the beginning or end
   */
  public Iterable<Entry<Key,Value>> singleRestrictedWildCardSearch(String exp) throws Exception {
    if (exp.indexOf("/") >= 0)
      throw new Exception("this method only works with unqualified names");

    Scanner scanner = conn.createScanner(tableName, auths);
    if (exp.startsWith("*")) {
      System.out.println("executing beginning wildcard search for " + exp);
      exp = exp.substring(1);
      scanner.setRange(Range.prefix(getReverseIndex(exp)));
    } else if (exp.endsWith("*")) {
      System.out.println("executing ending wildcard search for " + exp);
      exp = exp.substring(0, exp.length() - 1);
      scanner.setRange(Range.prefix(getForwardIndex(exp)));
    } else if (exp.indexOf("*") >= 0) {
      throw new Exception("this method only works for beginning or ending wild cards");
    } else {
      return exactTermSearch(exp);
    }
    return scanner;
  }

  /**
   * Scans over the index table for files or directories with a given name that can contain a single wildcard '*' anywhere in the term.
   *
   * @param exp
   *          the name a file or directory to search for with one optional wildcard '*'
   */
  public Iterable<Entry<Key,Value>> singleWildCardSearch(String exp) throws Exception {
    int starIndex = exp.indexOf("*");
    if (exp.indexOf("*", starIndex + 1) >= 0)
      throw new Exception("only one wild card for search");

    if (starIndex < 0) {
      return exactTermSearch(exp);
    } else if (starIndex == 0 || starIndex == exp.length() - 1) {
      return singleRestrictedWildCardSearch(exp);
    }

    String firstPart = exp.substring(0, starIndex);
    String lastPart = exp.substring(starIndex + 1);
    String regexString = ".*/" + exp.replace("*", "[^/]*");

    Scanner scanner = conn.createScanner(tableName, auths);
    if (firstPart.length() >= lastPart.length()) {
      System.out.println("executing middle wildcard search for " + regexString + " from entries starting with " + firstPart);
      scanner.setRange(Range.prefix(getForwardIndex(firstPart)));
    } else {
      System.out.println("executing middle wildcard search for " + regexString + " from entries ending with " + lastPart);
      scanner.setRange(Range.prefix(getReverseIndex(lastPart)));
    }
    IteratorSetting regex = new IteratorSetting(50, "regex", RegExFilter.class);
    RegExFilter.setRegexs(regex, null, null, regexString, null, false);
    scanner.addScanIterator(regex);
    return scanner;
  }

  public static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--path", description = "the directory to list")
    String path = "/";
    @Parameter(names = "--search", description = "find a file or directory with the given name")
    boolean search = false;
  }

  /**
   * Lists the contents of a directory using the directory table, or searches for file or directory names (if the -search flag is included).
   */
  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(QueryUtil.class.getName(), args);
    QueryUtil q = new QueryUtil(opts);
    if (opts.search) {
      for (Entry<Key,Value> e : q.singleWildCardSearch(opts.path)) {
        System.out.println(e.getKey().getColumnQualifier());
      }
    } else {
      for (Entry<String,Map<String,String>> e : q.getDirList(opts.path).entrySet()) {
        System.out.println(e);
      }
    }
  }
}
