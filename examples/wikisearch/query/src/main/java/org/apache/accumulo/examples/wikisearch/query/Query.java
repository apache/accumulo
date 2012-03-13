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
package org.apache.accumulo.examples.wikisearch.query;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.EJBException;
import javax.ejb.Local;
import javax.ejb.Stateless;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.examples.wikisearch.logic.ContentLogic;
import org.apache.accumulo.examples.wikisearch.logic.QueryLogic;
import org.apache.accumulo.examples.wikisearch.sample.Results;
import org.apache.log4j.Logger;

@Stateless
@Local(IQuery.class)
public class Query implements IQuery {
  
  private static final Logger log = Logger.getLogger(Query.class);
  
  // Inject values from XML configuration file
  @Resource(name = "instanceName")
  private String instanceName;
  
  @Resource(name = "zooKeepers")
  private String zooKeepers;
  
  @Resource(name = "username")
  private String username;
  
  @Resource(name = "password")
  private String password;
  
  @Resource(name = "tableName")
  private String tableName;
  
  @Resource(name = "threads")
  private int threads;
  
  private static final String XSL = "/accumulo-wikisearch/style.xsl";
  
  @PostConstruct
  public void init() {
    log.info("Post Construct");
  }
  
  @PreDestroy
  public void close() {
    log.info("Close called.");
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see sample.query.IQuery#html(java.lang.String, java.lang.String)
   */
  public String html(String query, String auths) {
    log.info("HTML query: " + query);
    URL u;
    try {
      u = new URL("http://" + System.getProperty("jboss.bind.address") + ":" + System.getProperty("jboss.web.http.port") + XSL);
    } catch (MalformedURLException e1) {
      throw new EJBException("Unable to load XSL stylesheet", e1);
    }
    InputStream xslContent;
    try {
      xslContent = u.openStream();
    } catch (IOException e1) {
      throw new EJBException("Unable to get xsl content", e1);
    }
    
    StringWriter xml = new StringWriter();
    StringWriter html = new StringWriter();
    
    Results results = query(query, auths);
    try {
      // Marshall the query results object
      JAXBContext ctx = JAXBContext.newInstance(Results.class);
      Marshaller m = ctx.createMarshaller();
      m.marshal(results, xml);
      
      // Perform XSL transform on the xml.
      StringReader reader = new StringReader(xml.toString());
      TransformerFactory tf = TransformerFactory.newInstance();
      // Create the transformer from the xsl
      Templates xsl = tf.newTemplates(new StreamSource(xslContent));
      Transformer t = xsl.newTransformer();
      t.transform(new StreamSource(reader), new StreamResult(html));
      
    } catch (Exception e) {
      throw new EJBException("Error processing query results", e);
    } finally {
      try {
        xslContent.close();
      } catch (IOException e) {
        throw new EJBException("Unable to close input stream", e);
      }
    }
    return html.toString();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see sample.query.IQuery#xml(java.lang.String, java.lang.String)
   */
  public Results xml(String query, String auths) {
    log.info("XML query: " + query);
    return query(query, auths);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see sample.query.IQuery#json(java.lang.String, java.lang.String)
   */
  public Results json(String query, String auths) {
    log.info("JSON query: " + query);
    return query(query, auths);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see sample.query.IQuery#yaml(java.lang.String, java.lang.String)
   */
  public Results yaml(String query, String auths) {
    log.info("YAML query: " + query);
    return query(query, auths);
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see sample.query.IQuery#content(java.lang.String, java.lang.String)
   */
  public Results content(String query, String auths) {
    log.info("Content query: " + query);
    Connector connector = null;
    if (null == instanceName || null == zooKeepers || null == username || null == password)
      throw new EJBException("Required parameters not set. [instanceName = " + this.instanceName + ", zookeepers = " + this.zooKeepers + ", username = "
          + this.username + ", password = " + this.password + "]. Check values in ejb-jar.xml");
    Instance instance = new ZooKeeperInstance(this.instanceName, this.zooKeepers);
    try {
      log.info("Connecting to [instanceName = " + this.instanceName + ", zookeepers = " + this.zooKeepers + ", username = " + this.username + ", password = "
          + this.password + "].");
      connector = instance.getConnector(this.username, this.password.getBytes());
    } catch (Exception e) {
      throw new EJBException("Error getting connector from instance", e);
    }
    
    // Create list of auths
    List<String> authorizations = new ArrayList<String>();
    if (auths != null && auths.length() > 0)
      for (String a : auths.split(","))
        authorizations.add(a);
    ContentLogic table = new ContentLogic();
    table.setTableName(tableName);
    return table.runQuery(connector, query, authorizations);
    
  }
  
  /**
   * calls the query logic with the parameters, returns results
   * 
   * @param query
   * @param auths
   * @return The results of a query
   * @throws ParseException
   */
  public Results query(String query, String auths) {
    
    Connector connector = null;
    if (null == instanceName || null == zooKeepers || null == username || null == password)
      throw new EJBException("Required parameters not set. [instanceName = " + this.instanceName + ", zookeepers = " + this.zooKeepers + ", username = "
          + this.username + ", password = " + this.password + "]. Check values in ejb-jar.xml");
    Instance instance = new ZooKeeperInstance(this.instanceName, this.zooKeepers);
    try {
      log.info("Connecting to [instanceName = " + this.instanceName + ", zookeepers = " + this.zooKeepers + ", username = " + this.username + ", password = "
          + this.password + "].");
      connector = instance.getConnector(this.username, this.password.getBytes());
    } catch (Exception e) {
      throw new EJBException("Error getting connector from instance", e);
    }
    
    // Create list of auths
    List<String> authorizations = new ArrayList<String>();
    if (auths != null && auths.length() > 0)
      for (String a : auths.split(","))
        authorizations.add(a);
    
    QueryLogic table = new QueryLogic();
    table.setTableName(tableName);
    table.setMetadataTableName(tableName + "Metadata");
    table.setIndexTableName(tableName + "Index");
    table.setReverseIndexTableName(tableName + "ReverseIndex");
    table.setQueryThreads(threads);
    table.setUnevaluatedFields("TEXT");
    table.setUseReadAheadIterator(false);
    return table.runQuery(connector, authorizations, query, null, null, null);
  }
  
}
