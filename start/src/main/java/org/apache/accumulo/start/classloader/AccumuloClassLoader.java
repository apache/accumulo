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
package org.apache.accumulo.start.classloader;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Deprecated
public class AccumuloClassLoader {

  public static final String GENERAL_CLASSPATHS = "general.classpaths";

  private static URL accumuloConfigUrl;
  private static URLClassLoader classloader;
  private static final Logger log = LoggerFactory.getLogger(AccumuloClassLoader.class);

  static {
    String configFile = System.getProperty("accumulo.properties", "accumulo.properties");
    if (configFile.startsWith("file://")) {
      try {
        File f = new File(new URI(configFile));
        if (f.exists() && !f.isDirectory()) {
          accumuloConfigUrl = f.toURI().toURL();
        } else {
          log.warn("Failed to load Accumulo configuration from " + configFile);
        }
      } catch (URISyntaxException | MalformedURLException e) {
        log.warn("Failed to load Accumulo configuration from " + configFile, e);
      }
    } else {
      accumuloConfigUrl = AccumuloClassLoader.class.getClassLoader().getResource(configFile);
      if (accumuloConfigUrl == null) {
        log.warn("Failed to load Accumulo configuration '{}' from classpath", configFile);
      }
    }
    if (accumuloConfigUrl != null) {
      log.debug("Using Accumulo configuration at {}", accumuloConfigUrl.getFile());
    }
  }

  /**
   * Returns value of property in accumulo.properties file, otherwise default value
   *
   * @param propertyName Name of the property to pull
   * @param defaultValue Value to default to if not found.
   * @return value of property or default
   */
  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD",
      justification = "url is specified by an admin, not unchecked user input")
  public static String getAccumuloProperty(String propertyName, String defaultValue) {
    if (accumuloConfigUrl == null) {
      log.warn(
          "Using default value '{}' for '{}' as there is no Accumulo configuration on classpath",
          defaultValue, propertyName);
      return defaultValue;
    }
    try {
      var config = new PropertiesConfiguration();
      try (var reader = new InputStreamReader(accumuloConfigUrl.openStream(), UTF_8)) {
        config.read(reader);
      }
      String value = config.getString(propertyName);
      if (value != null) {
        return value;
      }
      return defaultValue;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to look up property " + propertyName + " in " + accumuloConfigUrl.getFile(), e);
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
      classpath = (classpath.substring(0, envMatcher.start()) + varValue
          + classpath.substring(envMatcher.end()));
      envMatcher.reset(classpath);
    }
    return classpath;
  }

  /**
   * Populate the list of URLs with the items in the classpath string
   */
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "class path configuration is controlled by admin, not unchecked user input")
  private static void addUrl(String classpath, ArrayList<URL> urls) throws MalformedURLException {
    classpath = classpath.trim();
    if (classpath.isEmpty()) {
      return;
    }

    classpath = replaceEnvVars(classpath, System.getenv());

    // Try to make a URI out of the classpath
    URI uri = null;
    try {
      uri = new URI(classpath);
    } catch (URISyntaxException e) {
      // Not a valid URI
    }

    if (uri == null || !uri.isAbsolute()
        || (uri.getScheme() != null && uri.getScheme().equals("file://"))) {
      // Then treat this URI as a File.
      // This checks to see if the url string is a dir if it expand and get all jars in that
      // directory
      final File extDir = new File(classpath);
      if (extDir.isDirectory()) {
        urls.add(extDir.toURI().toURL());
      } else {
        if (extDir.getParentFile() != null) {
          var pattern = Pattern.compile(extDir.getName());
          File[] extJars =
              extDir.getParentFile().listFiles((dir, name) -> pattern.matcher(name).matches());
          if (extJars != null && extJars.length > 0) {
            for (File jar : extJars) {
              urls.add(jar.toURI().toURL());
            }
          } else {
            log.debug("ignoring classpath entry {}", classpath);
          }
        } else {
          log.debug("ignoring classpath entry {}", classpath);
        }
      }
    } else {
      urls.add(uri.toURL());
    }

  }

  private static ArrayList<URL> findAccumuloURLs() throws IOException {
    String cp = getAccumuloProperty(GENERAL_CLASSPATHS, null);
    if (cp == null) {
      return new ArrayList<>();
    }
    log.warn("'{}' is deprecated but was set to '{}' ", GENERAL_CLASSPATHS, cp);
    String[] cps = replaceEnvVars(cp, System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }

  public static synchronized ClassLoader getClassLoader() throws IOException {
    if (classloader == null) {
      ArrayList<URL> urls = findAccumuloURLs();

      ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();

      log.debug("Create 2nd tier ClassLoader using URLs: {}", urls);
      classloader =
          new URLClassLoader("AccumuloClassLoader (loads everything defined by general.classpaths)",
              urls.toArray(new URL[urls.size()]), parentClassLoader) {
            @Override
            protected synchronized Class<?> loadClass(String name, boolean resolve)
                throws ClassNotFoundException {

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
    }

    return classloader;
  }
}
