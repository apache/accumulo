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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.jci.listeners.AbstractFilesystemAlterationListener;
import org.apache.commons.jci.monitor.FilesystemAlterationObserver;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class builds a hierarchy of Classloaders in the form of:
 * 
 * <pre>
 * SystemClassLoader
 *       ^
 *       |
 * URLClassLoader that references the URLs for HADOOP_HOME, ZOOKEEPER_HOME, ACCUMULO_HOME and their associated directories
 *       ^
 *       |
 * URLClassLoader that references ACCUMULO_HOME/lib/ext, locations from the ACCUMULO property general.dynamic.classpaths and $ACCUMULO_XTRAJARS
 * </pre>
 * 
 * The last URLClassLoader in the chain monitors the directories for changes every 3 seconds (default value). If a change occurs, the last URLClassLoader is
 * abandoned and a new one is created. Objects that still reference the abandoned URLClassLoader will cause it to not be garbage collected until those objects
 * are garbage collected. If reload happens often enough it may cause excessive memory usage.
 * 
 */
public class AccumuloClassLoader {
  
  private static class Listener extends AbstractFilesystemAlterationListener {
    
    private volatile boolean firstCall = true;
    
    @Override
    public void onStop(FilesystemAlterationObserver pObserver) {
      
      super.onStop(pObserver);
      
      if (firstCall) {
        // the first time this is called it reports everything that exist as created
        // so there is no need to do anything
        synchronized (this) {
          firstCall = false;
          this.notifyAll();
        }
        return;
      }
      
      if (super.getChangedFiles().size() > 0 || super.getCreatedFiles().size() > 0 || super.getDeletedFiles().size() > 0
          || super.getChangedDirectories().size() > 0 || super.getCreatedDirectories().size() > 0 || super.getDeletedDirectories().size() > 0) {
        log.debug("Files have changed, setting loader to null ");
        loader = null;
      }
      
    }
    
    public synchronized void waitForFirstCall() {
      while (firstCall == true) {
        try {
          this.wait();
        } catch (InterruptedException e) {}
      }
    }
  }
  
  private static final Logger log = Logger.getLogger(AccumuloClassLoader.class);
  
  public static final String CLASSPATH_PROPERTY_NAME = "general.classpaths";
  
  public static final String DYNAMIC_CLASSPATH_PROPERTY_NAME = "general.dynamic.classpaths";
  
  public static final String ACCUMULO_CLASSPATH_VALUE = "$ACCUMULO_HOME/conf,\n" + "$ACCUMULO_HOME/lib/[^.].$ACCUMULO_VERSION.jar,\n"
      + "$ACCUMULO_HOME/lib/[^.].*.jar,\n" + "$ZOOKEEPER_HOME/zookeeper[^.].*.jar,\n" + "$HADOOP_HOME/[^.].*.jar,\n" + "$HADOOP_HOME/conf,\n"
      + "$HADOOP_HOME/lib/[^.].*.jar,\n";
  
  /**
   * Dynamic classpath. These locations will be monitored for changes.
   */
  public static final String DEFAULT_DYNAMIC_CLASSPATH_VALUE = "$ACCUMULO_HOME/lib/ext/[^.].*.jar\n";
  
  public static final String DEFAULT_CLASSPATH_VALUE = ACCUMULO_CLASSPATH_VALUE;
  
  private static final String SITE_CONF;
  static {
    String configFile = System.getProperty("org.apache.accumulo.config.file", "accumulo-site.xml");
    if (System.getenv("ACCUMULO_HOME") != null) {
      // accumulo home should be set
      SITE_CONF = System.getenv("ACCUMULO_HOME") + "/conf/" + configFile;
    } else {
      /*
       * if not it must be the build-server in which case I use a hack to get unittests working
       */
      String userDir = System.getProperty("user.dir");
      if (userDir == null)
        throw new RuntimeException("Property user.dir is not set");
      int index = userDir.indexOf("accumulo/");
      if (index >= 0) {
        String acuhome = userDir.substring(0, index + "accumulo/".length());
        SITE_CONF = acuhome + "/conf/" + configFile;
      } else {
        SITE_CONF = "/conf/" + configFile;
      }
    }
    
  }
  
  private static ClassLoader parent = null;
  private static volatile ClassLoader loader = null;
  private static AccumuloFilesystemAlterationMonitor monitor = null;
  private static Object lock = new Object();
  
  private static ArrayList<URL> findDynamicURLs() throws IOException {
    StringBuilder cp = new StringBuilder(getAccumuloDynamicClasspathStrings());
    String envJars = System.getenv("ACCUMULO_XTRAJARS");
    if (null != envJars && !envJars.equals(""))
      cp = cp.append(",").append(envJars);
    String[] cps = replaceEnvVars(cp.toString(), System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<URL>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }
  
  private static Set<File> findDirsFromUrls() throws IOException {
    Set<File> dirs = new HashSet<File>();
    StringBuilder cp = new StringBuilder(getAccumuloDynamicClasspathStrings());
    String envJars = System.getenv("ACCUMULO_XTRAJARS");
    if (null != envJars && !envJars.equals(""))
      cp = cp.append(",").append(envJars);
    String[] cps = replaceEnvVars(cp.toString(), System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<URL>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        classpath = classpath.trim();
        if (classpath.length() == 0)
          continue;
        
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
            urls.add(extDir.getAbsoluteFile().getParentFile().toURI().toURL());
          }
        } else {
          urls.add(uri.toURL());
        }
      }
    }
    
    for (URL url : urls) {
      try {
        File f = new File(url.toURI());
        if (!f.isDirectory())
          f = f.getParentFile();
        dirs.add(f);
      } catch (URISyntaxException e) {
        log.error("Unable to find directory for " + url + ", cannot create URI from it");
      }
    }
    return dirs;
  }
  
  private static ArrayList<URL> findAccumuloURLs() throws IOException {
    String cp = getAccumuloClasspathStrings();
    if (cp == null)
      return new ArrayList<URL>();
    String[] cps = replaceEnvVars(cp, System.getenv()).split(",");
    ArrayList<URL> urls = new ArrayList<URL>();
    for (String classpath : cps) {
      if (!classpath.startsWith("#")) {
        addUrl(classpath, urls);
      }
    }
    return urls;
  }
  
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
            public boolean accept(File dir, String name) {
              return name.matches("^" + extDir.getName());
            }
          });
          if (extJars != null && extJars.length > 0) {
            for (File jar : extJars)
              urls.add(jar.toURI().toURL());
          }
        }
      }
    } else {
      urls.add(uri.toURL());
    }
    
  }
  
  private static String replaceEnvVars(String classpath, Map<String,String> env) {
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
  
  public static String getAccumuloDynamicClasspathStrings() throws IllegalStateException {
    return getAccumuloString(DYNAMIC_CLASSPATH_PROPERTY_NAME, DEFAULT_DYNAMIC_CLASSPATH_VALUE);
  }
  
  public static String getAccumuloClasspathStrings() throws IllegalStateException {
    return getAccumuloString(CLASSPATH_PROPERTY_NAME, ACCUMULO_CLASSPATH_VALUE);
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
  
  private static String getAccumuloString(String propertyName, String defaultValue) {
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
   * Parses and XML Document for a property node for a <name> with the value propertyName if it finds one the function return that property's value for its
   * <value> node. If not found the function will return null
   * 
   * @param d
   *          XMLDocument to search through
   * @param propertyName
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
  
  public static void printClassPath() {
    try {
      System.out.println("Accumulo List of classpath items are:");
      for (URL url : findDynamicURLs()) {
        System.out.println(url.toExternalForm());
      }
      for (URL url : findAccumuloURLs()) {
        System.out.println(url.toExternalForm());
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }
  
  public synchronized static <U> Class<? extends U> loadClass(String classname, Class<U> extension) throws ClassNotFoundException {
    try {
      return (Class<? extends U>) Class.forName(classname, true, getClassLoader()).asSubclass(extension);
    } catch (IOException e) {
      throw new ClassNotFoundException("IO Error loading class " + classname, e);
    }
  }
  
  public static Class<?> loadClass(String classname) throws ClassNotFoundException {
    return loadClass(classname, Object.class).asSubclass(Object.class);
  }
  
  private static ClassLoader getAccumuloClassLoader() throws IOException {
    ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
    ArrayList<URL> accumuloURLs = findAccumuloURLs();
    log.debug("Create Dependency ClassLoader using URLs: " + accumuloURLs.toString());
    URLClassLoader aClassLoader = new URLClassLoader(accumuloURLs.toArray(new URL[accumuloURLs.size()]), parentClassLoader);
    return aClassLoader;
  }
  
  public static ClassLoader getClassLoader() throws IOException {
    ClassLoader localLoader = loader;
    while (null == localLoader) {
      synchronized (lock) {
        if (null == loader) {
          
          if (null != monitor) {
            monitor.stop();
            monitor = null;
          }
          
          if (null == parent)
            parent = getAccumuloClassLoader();
          
          // Find the dynamic classpath items
          final ArrayList<URL> dynamicURLs = findDynamicURLs();
          // Find the directories for the dynamic classpath items
          Set<File> monitoredDirs = findDirsFromUrls();
          
          // Setup the directory monitor
          monitor = new AccumuloFilesystemAlterationMonitor();
          Listener myListener = new Listener();
          for (File dir : monitoredDirs) {
            if (monitor.getListenersFor(dir) != null || monitor.getListenersFor(dir).length > 0) {
              log.debug("Monitor listening to " + dir.getAbsolutePath());
              monitor.addListener(dir, myListener);
            }
          }
          // setting this interval below 1 second is probably not helpful because the file modification time is in seconds
          monitor.setInterval(1000);
          monitor.start();
          myListener.waitForFirstCall();
          log.debug("Create Dynamic ClassLoader using URLs: " + dynamicURLs.toString());
          // the file system listner must run once to get the state of the filesystem, and it only detects changes on subsequent runs
          // we need to check to see if the set of files has changed since we started monitoring, so that we know we are monitoring all of the files
          // in the
          // class loader
          HashSet<URL> checkDynamicURLs = new HashSet<URL>(findDynamicURLs());
          HashSet<URL> originalDynamicURLs = new HashSet<URL>(dynamicURLs);
          if (checkDynamicURLs.equals(originalDynamicURLs)) {
            // file set has not changed, so we know that all of the dynamicURLs are being monitored for changes
            loader = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
              public ClassLoader run() {
                return new URLClassLoader(dynamicURLs.toArray(new URL[dynamicURLs.size()]), parent);
              }
            });
          }
        }
      }
      localLoader = loader;
    }
    return localLoader;
  }
  
}
