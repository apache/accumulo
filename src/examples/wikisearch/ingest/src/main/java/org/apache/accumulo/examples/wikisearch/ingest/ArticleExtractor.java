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
package org.apache.accumulo.examples.wikisearch.ingest;

import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.normalizer.NumberNormalizer;


public class ArticleExtractor {
  
  public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'Z");
  private static NumberNormalizer nn = new NumberNormalizer();
  private static LcNoDiacriticsNormalizer lcdn = new LcNoDiacriticsNormalizer();
  
  public static class Article {
    int id;
    String title;
    long timestamp;
    String comments;
    String text;
    
    private Article(int id, String title, long timestamp, String comments, String text) {
      super();
      this.id = id;
      this.title = title;
      this.timestamp = timestamp;
      this.comments = comments;
      this.text = text;
    }
    
    public int getId() {
      return id;
    }
    
    public String getTitle() {
      return title;
    }
    
    public String getComments() {
      return comments;
    }
    
    public String getText() {
      return text;
    }
    
    public long getTimestamp() {
      return timestamp;
    }
    
    public Map<String,Object> getFieldValues() {
      Map<String,Object> fields = new HashMap<String,Object>();
      fields.put("ID", this.id);
      fields.put("TITLE", this.title);
      fields.put("TIMESTAMP", this.timestamp);
      fields.put("COMMENTS", this.comments);
      return fields;
    }
    
    public Map<String,String> getNormalizedFieldValues() {
      Map<String,String> fields = new HashMap<String,String>();
      fields.put("ID", nn.normalizeFieldValue("ID", this.id));
      fields.put("TITLE", lcdn.normalizeFieldValue("TITLE", this.title));
      fields.put("TIMESTAMP", nn.normalizeFieldValue("TIMESTAMP", this.timestamp));
      fields.put("COMMENTS", lcdn.normalizeFieldValue("COMMENTS", this.comments));
      return fields;
    }
    
  }
  
  public ArticleExtractor() {}
  
  public Article extract(Reader reader) {
    XMLInputFactory xmlif = XMLInputFactory.newInstance();
    xmlif.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, Boolean.TRUE);
    
    XMLStreamReader xmlr = null;
    
    try {
      xmlr = xmlif.createXMLStreamReader(reader);
    } catch (XMLStreamException e1) {
      throw new RuntimeException(e1);
    }
    
    QName titleName = QName.valueOf("title");
    QName textName = QName.valueOf("text");
    QName revisionName = QName.valueOf("revision");
    QName timestampName = QName.valueOf("timestamp");
    QName commentName = QName.valueOf("comment");
    QName idName = QName.valueOf("id");
    
    Map<QName,StringBuilder> tags = new HashMap<QName,StringBuilder>();
    for (QName tag : new QName[] {titleName, textName, timestampName, commentName, idName}) {
      tags.put(tag, new StringBuilder());
    }
    
    StringBuilder articleText = tags.get(textName);
    StringBuilder titleText = tags.get(titleName);
    StringBuilder timestampText = tags.get(timestampName);
    StringBuilder commentText = tags.get(commentName);
    StringBuilder idText = tags.get(idName);
    
    StringBuilder current = null;
    boolean inRevision = false;
    while (true) {
      try {
        if (!xmlr.hasNext())
          break;
        xmlr.next();
      } catch (XMLStreamException e) {
        throw new RuntimeException(e);
      }
      QName currentName = null;
      if (xmlr.hasName()) {
        currentName = xmlr.getName();
      }
      if (xmlr.isStartElement() && tags.containsKey(currentName)) {
        if (!inRevision || (!currentName.equals(revisionName) && !currentName.equals(idName))) {
          current = tags.get(currentName);
          current.setLength(0);
        }
      } else if (xmlr.isStartElement() && currentName.equals(revisionName)) {
        inRevision = true;
      } else if (xmlr.isEndElement() && currentName.equals(revisionName)) {
        inRevision = false;
      } else if (xmlr.isEndElement() && current != null) {
        if (textName.equals(currentName)) {
          
          String title = titleText.toString();
          String text = articleText.toString();
          String comment = commentText.toString();
          int id = Integer.parseInt(idText.toString());
          long timestamp;
          try {
            timestamp = dateFormat.parse(timestampText.append("+0000").toString()).getTime();
            return new Article(id, title, timestamp, comment, text);
          } catch (ParseException e) {
            return null;
          }
        }
        current = null;
      } else if (current != null && xmlr.hasText()) {
        current.append(xmlr.getText());
      }
    }
    return null;
  }
}
