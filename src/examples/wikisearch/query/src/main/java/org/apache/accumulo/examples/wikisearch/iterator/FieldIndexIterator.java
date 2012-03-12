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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.examples.wikisearch.function.QueryFunctions;
import org.apache.accumulo.examples.wikisearch.util.FieldIndexKeyParser;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This iterator should only return keys from the fi\0{fieldName}:{fieldValue} part of the shard table. Expect topKey to be CF, {datatype}\0{UID}
 */
public class FieldIndexIterator extends WrappingIterator {
  
  private Key topKey = null;
  private Value topValue = null;
  private Range range = null;
  private Text currentRow;
  private Text fName = null;
  private String fNameString = null;
  private Text fValue = null;
  private String fOperator = null;
  private Expression expr = null;
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  protected static final Logger log = Logger.getLogger(FieldIndexIterator.class);
  private boolean negated = false;
  private int type;
  private static final String NULL_BYTE = "\0";
  private static final String ONE_BYTE = "\1";
  // According to the JEXL 2.0 docs, the engine is thread-safe. Let's create 1 engine per VM and
  // cache 128 expressions
  private static JexlEngine engine = new JexlEngine();
  private Range parentRange;
  private Text parentEndRow = null;
  private FieldIndexKeyParser keyParser;
  
  static {
    engine.setCache(128);
    Map<String,Object> functions = new HashMap<String,Object>();
    functions.put("f", QueryFunctions.class);
    engine.setFunctions(functions);
  }
  
  public static void setLogLevel(Level l) {
    log.setLevel(l);
  }
  
  // -------------------------------------------------------------------------
  // ------------- Constructors
  public FieldIndexIterator() {}
  
  public FieldIndexIterator(int type, Text rowId, Text fieldName, Text fieldValue, String operator) {
    this.fName = fieldName;
    this.fNameString = fName.toString().substring(3);
    this.fValue = fieldValue;
    this.fOperator = operator;
    this.range = buildRange(rowId);
    this.negated = false;
    this.type = type;
    
    // Create the Jexl expression, we need to add the ' around the field value
    StringBuilder buf = new StringBuilder();
    buf.append(fNameString).append(" ").append(this.fOperator).append(" ").append("'").append(fValue.toString()).append("'");
    this.expr = engine.createExpression(buf.toString());
    
    // Set a default KeyParser
    keyParser = createDefaultKeyParser();
  }
  
  public FieldIndexIterator(int type, Text rowId, Text fieldName, Text fieldValue, boolean neg, String operator) {
    this.fName = fieldName;
    this.fNameString = fName.toString().substring(3);
    this.fValue = fieldValue;
    this.fOperator = operator;
    this.range = buildRange(rowId);
    this.negated = neg;
    this.type = type;
    
    // Create the Jexl expression, we need to add the ' around the field value
    StringBuilder buf = new StringBuilder();
    buf.append(fNameString).append(" ").append(this.fOperator).append(" ").append("'").append(fValue.toString()).append("'");
    this.expr = engine.createExpression(buf.toString());
    // Set a default KeyParser
    keyParser = createDefaultKeyParser();
  }
  
  public FieldIndexIterator(FieldIndexIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    // Set a default KeyParser
    keyParser = createDefaultKeyParser();
  }
  
  private FieldIndexKeyParser createDefaultKeyParser() {
    FieldIndexKeyParser parser = new FieldIndexKeyParser();
    return parser;
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new FieldIndexIterator(this, env);
  }
  
  @Override
  public Key getTopKey() {
    return topKey;
  }
  
  @Override
  public Value getTopValue() {
    return topValue;
  }
  
  @Override
  public boolean hasTop() {
    return (topKey != null);
  }
  
  @Override
  public void next() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("next()");
    }
    if (this.hasTop()) {
      currentRow = topKey.getRow();
    }
    
    getSource().next();
    while (true) {
      log.debug("next(), Range: " + range);
      if (getSource().hasTop()) {
        Key k = getSource().getTopKey();
        if (range.contains(k)) {
          if (matches(k)) {
            topKey = k;
            topValue = getSource().getTopValue();
            return;
          } else {
            getSource().next();
          }
          
        } else {
          
          if (parentEndRow != null) { // need to check it
            if (k.getRow().equals(currentRow)) {
              currentRow = getNextRow();
            } else if (currentRow == null || k.getRow().compareTo(currentRow) > 0) {
              currentRow = k.getRow();
            }
            
            if (currentRow == null || parentEndRow.compareTo(currentRow) < 0) {
              // you're done
              topKey = null;
              topValue = null;
              return;
            }
            
          } else { // we can go to end of the tablet
            if (k.getRow().equals(currentRow)) {
              currentRow = getNextRow();
              if (currentRow == null) {
                topKey = null;
                topValue = null;
                return;
              }
            } else if (currentRow == null || (k.getRow().compareTo(currentRow) > 0)) {
              currentRow = k.getRow();
            }
          }
          
          // construct new range and seek the source
          range = buildRange(currentRow);
          if (log.isDebugEnabled()) {
            log.debug("next, range: " + range);
          }
          getSource().seek(range, EMPTY_COL_FAMS, false);
        }
      } else {
        topKey = null;
        topValue = null;
        return;
      }
    }
  }
  
  /*
   * NOTE: there is some special magic here with range modification. If it's negated, assume the range is explicitly set and don't mess with it (this is how
   * it's called by the BooleanLogicIterator) Otherwise, modify the range to start at the beginning and set an explicit end point.
   * 
   * In the future, maybe all we need to do is look for an endKey and modifying that.
   */
  @Override
  public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    parentRange = r;
    if (log.isDebugEnabled()) {
      log.debug("begin seek, range: " + r);
    }
    if (parentRange.getEndKey() != null) {
      if (parentRange.getEndKey().getRow() != null) {
        parentEndRow = parentRange.getEndKey().getRow();
        if (log.isDebugEnabled()) {
          log.debug("begin seek, parentEndRow: " + parentEndRow);
        }
      }
    }
    
    try {
      if (isNegated()) {
        range = r;
        if (log.isDebugEnabled()) {
          log.debug("seek, negation, skipping range modification.");
        }
      } else {
        if (r.getStartKey() != null) {
          if (r.getStartKey().getRow() == null || r.getStartKey().getRow().toString().isEmpty()) {
            currentRow = getFirstRow();
          } else {
            currentRow = r.getStartKey().getRow();
          }
          this.range = buildRange(currentRow);
        } else {
          currentRow = getFirstRow();
          this.range = buildRange(currentRow);
        }
      }
      
      setTopKey(null);
      setTopValue(null);
      
      if (log.isDebugEnabled()) {
        log.debug("seek, incoming range: " + range);
      }
      getSource().seek(range, columnFamilies, inclusive);
      
      while (topKey == null) {
        if (getSource().hasTop()) {
          if (log.isDebugEnabled()) {
            log.debug("seek, source has top: " + getSource().getTopKey());
          }
          Key k = getSource().getTopKey();
          if (range.contains(k)) {
            if (matches(k)) {
              topKey = k;
              topValue = getSource().getTopValue();
              if (log.isDebugEnabled()) {
                log.debug("seek, source has top in valid range");
              }
            } else {
              getSource().next();
            }
          } else {
            if (log.isDebugEnabled()) {
              log.debug("seek, top out of range");
              String pEndRow = "empty";
              if (parentEndRow != null) {
                pEndRow = parentEndRow.toString();
              }
              log.debug("source.topKey.row: " + k.getRow() + "\t currentRow: " + currentRow + "\t parentEndRow: " + pEndRow);
            }
            if (isNegated()) {
              topKey = null;
              topValue = null;
              return;
            }
            
            if (parentEndRow != null) {
              // check it
              if (k.getRow().equals(currentRow)) {
                currentRow = getNextRow();
              }
              
              if (currentRow == null || parentEndRow.compareTo(currentRow) < 0) {
                // you're done
                topKey = null;
                topValue = null;
                return;
              }
              
            } else { // can go to end of the tablet
              if (k.getRow().equals(currentRow)) {
                currentRow = getNextRow();
                if (currentRow == null) {
                  topKey = null;
                  topValue = null;
                  return;
                }
              }
            }
            
            // construct new range and seek the source
            range = buildRange(currentRow);
            if (log.isDebugEnabled()) {
              log.debug("currentRow: " + currentRow);
              log.debug("seek, range: " + range);
            }
            getSource().seek(range, columnFamilies, inclusive);
          }
        } else {
          if (log.isDebugEnabled()) {
            log.debug("seek, underlying source had no top key.");
          }
          topKey = null;
          topValue = null;
          return;
        }
      }
      if (log.isDebugEnabled()) {
        log.debug("seek, topKey found: " + topKey);
      }
    } catch (IOException e) {
      topKey = null;
      topValue = null;
      throw new IOException();
    }
  }
  
  // -------------------------------------------------------------------------
  // ------------- Public stuff
  public boolean isNegated() {
    return negated;
  }
  
  public Text getCurrentRow() {
    return currentRow;
  }
  
  public Text getfName() {
    return fName;
  }
  
  public Text getfValue() {
    return fValue;
  }
  
  // works like seek, but we need to avoid range issues.
  public boolean jump(Key jumpKey) throws IOException {
    if (log.isDebugEnabled()) {
      String pEndRow = "empty";
      if (parentEndRow != null) {
        pEndRow = parentEndRow.toString();
      }
      log.debug("jump, current range: " + range + "  parentEndRow is: " + pEndRow);
      
    }
    
    if (parentEndRow != null && jumpKey.getRow().compareTo(parentEndRow) > 0) {
      // can't go there.
      if (log.isDebugEnabled()) {
        log.debug("jumpRow: " + jumpKey.getRow() + " is greater than my parentEndRow: " + parentEndRow);
      }
      return false;
    }
    
    int comp;
    if (!this.hasTop()) {
      if (log.isDebugEnabled()) {
        log.debug("current row: " + this.currentRow);
      }
      
      /*
       * if I don't have a top, then I should be out of my range for my current row. Need to check parent range to see if I'm supposed to continue to next row
       * or not. Current row can be null because maybe I never found anything in this row.
       */
      
      if (parentEndRow != null) {
        // if jumpKey row is greater than parentEndRow, stop
        if (jumpKey.getRow().compareTo(parentEndRow) > 0) {
          if (log.isDebugEnabled()) {
            log.debug("jumpKey row is greater than my parentEndRow, done");
          }
          return false;
        }
        
        // if my current row is null, I must have hit the end of the tablet
        if (currentRow == null) {
          if (log.isDebugEnabled()) {
            log.debug("I have parentEndRow, but no current row, must have hit end of tablet, done");
          }
          return false;
        }
        
        // if my current row is greater than jump row stop, a seek will be
        // called to get me going again. If my row is equal, but i don't
        // have a topkey, i'm done
        if (currentRow.compareTo(jumpKey.getRow()) >= 0) {
          if (log.isDebugEnabled()) {
            log.debug("I have parentEndRow, but topKey, and my currentRow is >= jumpRow, done");
          }
          return false;
        }
        
      } else { // we're allowed to go to the end of the tablet
        // if my current row is null, I must have hit the end of the tablet
        if (currentRow == null) {
          if (log.isDebugEnabled()) {
            log.debug("no parentEndRow and current Row is null, must have hit end of tablet, done");
          }
          return false;
        }
        
        if (currentRow.compareTo(jumpKey.getRow()) >= 0) {
          // i'm past or equal to the jump point and have no top,
          // jumping's not going to help
          if (log.isDebugEnabled()) {
            log.debug("no parentEndRow, no topKey, and currentRow is >= jumpRow, done");
          }
          return false;
        }
      }
      
      // ok, jumpKey is ahead of me I'll mark it and allow the normal
      // flow to jump there and see if I have top.
      if (log.isDebugEnabled()) {
        log.debug("no topKey, but jumpRow is ahead and I'm allowed to go to it, marking");
      }
      comp = -1;
      
    } else { // I have a topKey, I can do the normal comparisons
      if (log.isDebugEnabled()) {
        log.debug("have top, can do normal comparisons");
      }
      comp = this.topKey.getRow().compareTo(jumpKey.getRow());
    }
    
    // ------------------
    // compare rows
    if (comp > 0) { // my row is ahead of jump key
      if (canBeInNextRow()) {
        if (log.isDebugEnabled()) {
          log.debug("I'm ahead of jump row & it's ok.");
          log.debug("jumpRow: " + jumpKey.getRow() + " myRow: " + topKey.getRow() + " parentEndRow: " + parentEndRow);
        }
        return true;
      } else {
        if (log.isDebugEnabled()) {
          log.debug("I'm ahead of jump row & can't be here, or at end of tablet.");
        }
        topKey = null;
        topValue = null;
        return false;
      }
      
    } else if (comp < 0) { // a row behind jump key, need to move forward
      if (log.isDebugEnabled()) {
        String myRow = "";
        if (hasTop()) {
          myRow = topKey.getRow().toString();
        } else if (currentRow != null) {
          myRow = currentRow.toString();
        }
        log.debug("My row " + myRow + " is less than jump row: " + jumpKey.getRow() + " seeking");
      }
      range = buildRange(jumpKey.getRow());
      // this.seek(range, EMPTY_COL_FAMS, false);
      
      boolean success = jumpSeek(range);
      if (log.isDebugEnabled() && success) {
        log.debug("uid forced jump, found topKey: " + topKey);
      }
      
      if (!this.hasTop()) {
        log.debug("seeked with new row and had no top");
        topKey = null;
        topValue = null;
        return false;
      } else if (parentEndRow != null && currentRow.compareTo(parentEndRow) > 0) {
        if (log.isDebugEnabled()) {
          log.debug("myRow: " + getTopKey().getRow() + " is past parentEndRow: " + parentEndRow);
        }
        topKey = null;
        topValue = null;
        return false;
      }
      if (log.isDebugEnabled()) {
        log.debug("jumped, valid top: " + getTopKey());
      }
      
      return true;
      
    } else { // rows are equal, check the uid!
    
      keyParser.parse(topKey);
      String myUid = keyParser.getUid();
      keyParser.parse(jumpKey);
      String jumpUid = keyParser.getUid();
      
      int ucomp = myUid.compareTo(jumpUid);
      if (log.isDebugEnabled()) {
        log.debug("topKeyUid: " + myUid + "  jumpUid: " + jumpUid + "  myUid.compareTo(jumpUid)->" + ucomp);
      }
      if (ucomp < 0) { // need to move up
        log.debug("my uid is less than jumpUid, topUid: " + myUid + "   jumpUid: " + jumpUid);
        
        Text cq = jumpKey.getColumnQualifier();
        int index = cq.find(NULL_BYTE);
        if (0 <= index) {
          cq.set(cq.getBytes(), index + 1, cq.getLength() - index - 1);
        } else {
          log.error("Expected a NULL separator in the column qualifier");
          this.topKey = null;
          this.topValue = null;
          return false;
        }

        // note my internal range stays the same, I just need to move forward
        Key startKey = new Key(topKey.getRow(), fName, new Text(fValue + NULL_BYTE + cq));
        Key endKey = new Key(topKey.getRow(), fName, new Text(fValue + ONE_BYTE));
        range = new Range(startKey, true, endKey, false);
        log.debug("Using range: " + range + " to seek");
        // source.seek(range, EMPTY_COL_FAMS, false);
        boolean success = jumpSeek(range);
        if (log.isDebugEnabled() && success) {
          log.debug("uid forced jump, found topKey: " + topKey);
        }
        
        return success;
        
      } else { // else do nothing
        log.debug("my uid is greater than jumpUid, topKey: " + topKey + "   jumpKey: " + jumpKey);
        log.debug("doing nothing");
      }
    }
    
    return hasTop();
  }
  
  // -------------------------------------------------------------------------
  // ------------- Private stuff, KEEP OUT!!
  private void setTopKey(Key key) {
    topKey = key;
  }
  
  private void setTopValue(Value v) {
    this.topValue = v;
  }
  
  private boolean canBeInNextRow() {
    if (parentEndRow == null) {
      return true;
    } else if (currentRow == null) {
      return false;
    } else if (currentRow.compareTo(parentEndRow) <= 0) {
      return true;
    } else {
      return false;
    }
  }
  
  private Range buildRange(Text rowId) {
    if (type == ParserTreeConstants.JJTGTNODE || type == ParserTreeConstants.JJTGENODE || type == ParserTreeConstants.JJTLTNODE
        || type == ParserTreeConstants.JJTLENODE || type == ParserTreeConstants.JJTERNODE || type == ParserTreeConstants.JJTNRNODE) {
      Key startKey = new Key(rowId, fName);
      Key endKey = new Key(rowId, new Text(fName + NULL_BYTE));
      return (new Range(startKey, true, endKey, false));
    } else {
      // construct new range
      Key startKey = new Key(rowId, fName, new Text(fValue + NULL_BYTE));
      Key endKey = new Key(rowId, fName, new Text(fValue + ONE_BYTE));
      return (new Range(startKey, true, endKey, false));
    }
  }
  
  // need to build a range starting at the end of current row and seek the
  // source to it. If we get an IOException, that means we hit the end of the tablet.
  private Text getNextRow() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("getNextRow()");
    }
    Key fakeKey = new Key(new Text(currentRow + NULL_BYTE));
    Range fakeRange = new Range(fakeKey, fakeKey);
    getSource().seek(fakeRange, EMPTY_COL_FAMS, false);
    if (getSource().hasTop()) {
      return getSource().getTopKey().getRow();
    } else {
      return null;
    }
  }
  
  private Text getFirstRow() throws IOException {
    getSource().seek(new Range(), EMPTY_COL_FAMS, false);
    if (getSource().hasTop()) {
      return getSource().getTopKey().getRow();
    } else {
      throw new IOException();
    }
  }
  
  private boolean matches(Key k) {
    if (log.isDebugEnabled()) {
      log.debug("You've reached the match function!");
    }
    JexlContext ctx = new MapContext();
    // Add the field value from the key to the context
    // String fieldValue = k.getColumnQualifier().toString().split("\0")[0];
    // String fieldValue = getFieldValueFromKey(k);
    keyParser.parse(k);
    String fieldValue = keyParser.getFieldValue();
    
    ctx.set(fNameString, fieldValue);
    Object o = expr.evaluate(ctx);
    if (o instanceof Boolean && (((Boolean) o) == true)) {
      if (log.isDebugEnabled()) {
        log.debug("matches:: fName: " + fName + " , fValue: " + fieldValue + " ,  operator: " + fOperator + " , key: " + k);
      }
      return true;
    } else {
      if (log.isDebugEnabled()) {
        log.debug("NO MATCH:: fName: " + fName + " , fValue: " + fieldValue + " ,  operator: " + fOperator + " , key: " + k);
      }
      return false;
    }
  }
  
  private boolean jumpSeek(Range r) throws IOException {
    range = r;
    setTopKey(null);
    setTopValue(null);
    getSource().seek(range, EMPTY_COL_FAMS, false);
    while (topKey == null) {
      if (getSource().hasTop()) {
        if (log.isDebugEnabled()) {
          log.debug("jump, source has top: " + getSource().getTopKey());
        }
        Key k = getSource().getTopKey();
        if (range.contains(k)) {
          if (matches(k)) {
            topKey = k;
            topValue = getSource().getTopValue();
            if (log.isDebugEnabled()) {
              log.debug("jump, source has top in valid range");
            }
          } else {
            getSource().next();
          }
        } else {
          if (log.isDebugEnabled()) {
            log.debug("jump, top out of range");
            String pEndRow = "empty";
            if (parentEndRow != null) {
              pEndRow = parentEndRow.toString();
            }
            log.debug("source.topKey.row: " + k.getRow() + "\t currentRow: " + currentRow + "\t parentEndRow: " + pEndRow);
          }
          
          if (parentEndRow != null) {
            
            if (currentRow == null) {
              topKey = null;
              topValue = null;
              return false;
            }
            // check it
            if (k.getRow().equals(currentRow)) {
              currentRow = getNextRow();
            } else if (k.getRow().compareTo(currentRow) > 0) {
              currentRow = k.getRow();
            }
            
            if (currentRow == null || parentEndRow.compareTo(currentRow) < 0) {
              // you're done
              topKey = null;
              topValue = null;
              return false;
            }
            
          } else { // can go to end of the tablet
            if (currentRow == null || k.getRow() == null) {
              topKey = null;
              topValue = null;
              return false;
            }
            
            if (k.getRow().equals(currentRow)) {
              currentRow = getNextRow();
              if (currentRow == null) {
                topKey = null;
                topValue = null;
                return false;
              }
            } else if (k.getRow().compareTo(currentRow) > 0) {
              currentRow = k.getRow();
            }
          }
          
          // construct new range and seek the source
          range = buildRange(currentRow);
          if (log.isDebugEnabled()) {
            log.debug("jump, new build range: " + range);
          }
          getSource().seek(range, EMPTY_COL_FAMS, false);
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("jump, underlying source had no top key.");
        }
        topKey = null;
        topValue = null;
        return false;
      }
    }// end while
    
    return hasTop();
  }
}
