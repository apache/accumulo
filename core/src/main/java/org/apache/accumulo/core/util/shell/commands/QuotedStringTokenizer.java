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
package org.apache.accumulo.core.util.shell.commands;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.accumulo.core.util.BadArgumentException;

/**
 * A basic tokenizer for generating tokens from a string. It understands quoted strings and escaped quote characters.
 * 
 * You can use the escape sequence '\' to escape single quotes, double quotes, and spaces only, in addition to the escape character itself.
 * 
 * The behavior is the same for single and double quoted strings. (i.e. '\'' is the same as "\'")
 */

public class QuotedStringTokenizer implements Iterable<String> {
  private ArrayList<String> tokens;
  private String input;
  
  public QuotedStringTokenizer(String t) throws BadArgumentException {
    tokens = new ArrayList<String>();
    this.input = t;
    createTokens();
  }
  
  public String[] getTokens() {
    return tokens.toArray(new String[tokens.size()]);
  }
  
  private void createTokens() throws BadArgumentException {
    boolean inQuote = false;
    boolean inEscapeSequence = false;
    String hexChars = null;
    char inQuoteChar = '"';
    StringBuilder sb = new StringBuilder();
    
    for (int i = 0; i < input.length(); ++i) {
      char ch = input.charAt(i);
      
      // if I ended up in an escape sequence, check for valid escapable character, and add it as a literal
      if (inEscapeSequence) {
        inEscapeSequence = false;
        if (ch == 'x')
          hexChars = "";
        else if (ch == ' ' || ch == '\'' || ch == '"' || ch == '\\')
          sb.append(ch);
        else
          throw new BadArgumentException("can only escape single quotes, double quotes, the space character, the backslash, and hex input", input, i);
      }
      // in a hex escape sequence
      else if (hexChars != null) {
        int digit = Character.digit(ch, 16);
        if (digit < 0)
          throw new BadArgumentException("expected hex character", input, i);
        hexChars += ch;
        if (hexChars.length() == 2) {
          byte b;
          try {
            b = (byte) (0xff & Short.parseShort(hexChars, 16));
            if (!Character.isValidCodePoint(b))
              throw new NumberFormatException();
          } catch (NumberFormatException e) {
            throw new BadArgumentException("unsupported non-ascii character", input, i);
          }
          sb.append((char) b);
          hexChars = null;
        }
      }
      // in a quote, either end the quote, start escape, or continue a token
      else if (inQuote) {
        if (ch == inQuoteChar) {
          inQuote = false;
          tokens.add(sb.toString());
          sb = new StringBuilder();
        } else if (ch == '\\')
          inEscapeSequence = true;
        else
          sb.append(ch);
      }
      // not in a quote, either enter a quote, end a token, start escape, or continue a token
      else {
        if (ch == '\'' || ch == '"') {
          if (sb.length() > 0) {
            tokens.add(sb.toString());
            sb = new StringBuilder();
          }
          inQuote = true;
          inQuoteChar = ch;
        } else if (ch == ' ' && sb.length() > 0) {
          tokens.add(sb.toString());
          sb = new StringBuilder();
        } else if (ch == '\\')
          inEscapeSequence = true;
        else if (ch != ' ')
          sb.append(ch);
      }
    }
    if (inQuote)
      throw new BadArgumentException("missing terminating quote", input, input.length());
    else if (inEscapeSequence || hexChars != null)
      throw new BadArgumentException("escape sequence not complete", input, input.length());
    if (sb.length() > 0)
      tokens.add(sb.toString());
  }
  
  @Override
  public Iterator<String> iterator() {
    return tokens.iterator();
  }
}
