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
package org.apache.accumulo.start.classloader;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 */
public class AccumuloClassLoader {

  public static final String CLASSPATH_PROPERTY_NAME = "general.classpaths";

  /* @formatter:off */
  public static final String ACCUMULO_CLASSPATH_VALUE =
      "$ACCUMULO_CONF_DIR,\n" +
          "$ACCUMULO_HOME/lib/[^.].*.jar,\n" +
          "$ZOOKEEPER_HOME/zookeeper[^.].*.jar,\n" +
          "$HADOOP_CONF_DIR,\n" +
          "$HADOOP_PREFIX/[^.].*.jar,\n" +
          "$HADOOP_PREFIX/lib/[^.].*.jar,\n" +
          "$HADOOP_PREFIX/share/hadoop/common/.*.jar,\n" +
          "$HADOOP_PREFIX/share/hadoop/common/lib/.*.jar,\n" +
          "$HADOOP_PREFIX/share/hadoop/hdfs/.*.jar,\n" +
          "$HADOOP_PREFIX/share/hadoop/mapreduce/.*.jar,\n" +
          "/usr/lib/hadoop/[^.].*.jar,\n" +
          "/usr/lib/hadoop/lib/[^.].*.jar,\n" +
          "/usr/lib/hadoop-hdfs/[^.].*.jar,\n" +
          "/usr/lib/hadoop-mapreduce/[^.].*.jar,\n" +
          "/usr/lib/hadoop-yarn/[^.].*.jar,\n"
          ;
  /* @formatter:on */

  public static final String MAVEN_PROJECT_BASEDIR_PROPERTY_NAME = "general.maven.project.basedir";
  public static final String DEFAULT_MAVEN_PROJECT_BASEDIR_VALUE = "";

  private static String SITE_CONF;

  private static URLClassLoader classloader;

  private static Logger log = Logger.getLogger(AccumuloClassLoader.class);

  static {
    String configFile = System.getProperty("org.apache.accumulo.config.file", "accumulo-site.xml");
    if (System.getenv("ACCUMULO_CONF_DIR") != null) {
      // accumulo conf dir should be set
      SITE_CONF = System.getenv("ACCUMULO_CONF_DIR") + "/" + configFile;
    } else if (System.getenv("ACCUMULO_HOME") != null) {
      // if no accumulo conf dir, try accumulo home default
      SITE_CONF = System.getenv("ACCUMULO_HOME") + "/conf/" + configFile;
    } else {
      SITE_CONF = null;
    }
  }

  /**
   * Parses and XML Document for a property node for a &lt;name&gt; with the value propertyName if it finds one the function return that property's value for its
   * &lt;value&gt; node. If not found the function will return null
   *
   * @param d
   *          XMLDocument to search through
   */
  private static String getAccumuloClassPathStrings(Document d, String propertyName) {
    NodeList pnodes = d.getElementsByTagName("property");
    for (int i = pnodes.getLength() - 1; i >= 0; i--) {
      Element current_property = (Element) pnodes.item(i);
      Node cname = current_property.getElementsByTagName("name").item(0);
      if (cname != null && cname.getTextContent().compareTo(propertyName) == 0) {
        Node cvalue = current_property.getElementsByTagName("value").item(0);
        if (cvalue != null) {
          return cvalue.getTextContent();
        }
      }
    }
    return null;
  }

  /**
   * Looks for the site configuration file for Accumulo and if it has a property for propertyName return it otherwise returns defaultValue Should throw an
   * exception if the default configuration can not be read;
   *
   * @param propertyName
   *          Name of the property to pull
   * @param defaultValue
   *          Value to default to if not found.
   * @return site or default class path String
   */

  public static String getAccumuloString(String propertyName, String defaultValue) {

    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      String site_classpath_string = null;
      try {
        Document site_conf = db.parse(SITE_CONF);
        site_classpath_string = getAccumuloClassPathStrings(site_conf, propertyName);
      } catch (Exception e) {
        /* we don't care because this is optional and we can use defaults */
      }
      if (site_classpath_string != null)
        return site_classpath_string;
      return defaultValue;
    } catch (Exception e) {
      throw new IllegalStateException("ClassPath Strings Lookup failed", e);
    }
  }

  /**
   * Replace environment variables in the classpath string with their actual value
   */
  public static String replaceEnvVars(String classpath, Map<String,String> env) {
    Pattern envPat = Pattern.compile("\\$[A-Za-z][a-zA-Z0-9_]*");
    Matcher envMatcher = envPat.matcher(classpath);
    while (envMatcher.find(0)) {
      // name comes after the '$'
      String varName = envMatcher.group().substring(1);
      String varValue = env.get(varName);
      if (varValue == null) {
        varValue = "";
      }
      classpath = (classpath.substring(0, envMatcher.start()) + varValue + classpath.substring(envMatcher.end()));
      envMatcher.reset(classpath);
    }
    return classpath;
  }

  /**
   * Populate the list of URLs with the items in the classpath string
   */
  private static void addUrl(String classpath, ArrayList<URL> urls) throws MalformedURLException {
    classpath = classpath.trim();
    if (classpath.length() == 0)
      return;

    classpath = replaceEnvVars(classpath, System.getenv());

    // Try to make a URI out of the classpath
    URI uri = null;
    try {
      uri = new URI(classpath);
    } catch (URISyntaxException e) {
      // Not a valid URI
    }

    if (null == uri || !uri.isAbsolute() || (null != uri.getScheme() && uri.getScheme().equals("file://"))) {
      // Then treat this URI as a File.
      // This checks to see if the url string is a dir if it expand and get all jars in that directory
      final File extDir = new File(classpath);
      if (extDir.isDirectory())
        urls.add(extDir.toURI().toURL());
      else {
        if (extDir.getParentFile() != null) {
          File[] extJars = extDir.getParentFile().listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              return name.matches("^" + extDir.getName());
            }
          });
          if (extJars != null && extJars.length > 0) {
            for (File jar : extJars)
              urls.add(jar.toURI().toURL());
          } else {
            log.debug("ignoring classpath entry " + classpath);
          }
        } else {
          log.debug("ignoring classpath entry " + classpath);
        }
      }
    } else {
      urls.add(uri.toURL());
    }

  }

  private static ArrayList<URL> findAccumuloURLs() throws IOException {
    String cp = getAccumuloString(AccumuloClassLoader.CLASSPATH_PROPERTY_NAME, AccumuloClassLoader.ACCUMULO_CLASSPATH_VALUE);
    if (cp == null)
      return new ArrayList<URL>();
    String[] cps = replaceEnvVars(cp, System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<URL>();
    for (String classpath : getMavenClasspaths())
      addUrl(classpath, urls);
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }

  private static Set<String> getMavenClasspaths() {
    String baseDirname = AccumuloClassLoader.getAccumuloString(MAVEN_PROJECT_BASEDIR_PROPERTY_NAME, DEFAULT_MAVEN_PROJECT_BASEDIR_VALUE);
    if (baseDirname == null || baseDirname.trim().isEmpty())
      return Collections.emptySet();
    Set<String> paths = new TreeSet<String>();
    findMavenTargetClasses(paths, new File(baseDirname.trim()), 0);
    return paths;
  }

  private static void findMavenTargetClasses(Set<String> paths, File file, int depth) {
    if (depth > 3)
      return;
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      for (File child : children)
        findMavenTargetClasses(paths, child, depth + 1);
    } else if ("pom.xml".equals(file.getName())) {
      paths.add(file.getParentFile().getAbsolutePath() + File.separator + "target" + File.separator + "classes");
    }
  }

  public static synchronized ClassLoader getClassLoader() throws IOException {
    if (classloader == null) {
      ArrayList<URL> urls = findAccumuloURLs();

      ClassLoader parentClassLoader = AccumuloClassLoader.class.getClassLoader();

      log.debug("Create 2nd tier ClassLoader using URLs: " + urls.toString());
      URLClassLoader aClassLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]), parentClassLoader) {
        @Override
        protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {

          if (name.startsWith("org.apache.accumulo.start.classloader.vfs")) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
              try {
                // try finding this class here instead of parent
                findClass(name);
              } catch (ClassNotFoundException e) {}
            }
          }
          return super.loadClass(name, resolve);
        }
      };
      classloader = aClassLoader;
    }

    return classloader;
  }
}
