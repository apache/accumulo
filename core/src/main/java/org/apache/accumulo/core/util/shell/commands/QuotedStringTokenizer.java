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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.shell.Shell;

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
    try {
      createTokens();
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
  
  public String[] getTokens() {
    return tokens.toArray(new String[tokens.size()]);
  }
  
  private void createTokens() throws BadArgumentException, UnsupportedEncodingException {
    boolean inQuote = false;
    boolean inEscapeSequence = false;
    String hexChars = null;
    char inQuoteChar = '"';
    
    byte[] token = new byte[input.length()];
    int tokenLength = 0;
    byte[] inputBytes = input.getBytes();
    for (int i = 0; i < input.length(); ++i) {
      char ch = input.charAt(i);
      
      // if I ended up in an escape sequence, check for valid escapable character, and add it as a literal
      if (inEscapeSequence) {
        inEscapeSequence = false;
        if (ch == 'x')
          hexChars = "";
        else if (ch == ' ' || ch == '\'' || ch == '"' || ch == '\\')
          token[tokenLength++] = inputBytes[i];
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
            if (!Character.isValidCodePoint(0xff & b))
              throw new NumberFormatException();
          } catch (NumberFormatException e) {
            throw new BadArgumentException("unsupported non-ascii character", input, i);
          }
          token[tokenLength++] = b;
          hexChars = null;
        }
      }
      // in a quote, either end the quote, start escape, or continue a token
      else if (inQuote) {
        if (ch == inQuoteChar) {
          inQuote = false;
          tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
          tokenLength = 0;
        } else if (ch == '\\')
          inEscapeSequence = true;
        else
          token[tokenLength++] = inputBytes[i];
      }
      // not in a quote, either enter a quote, end a token, start escape, or continue a token
      else {
        if (ch == '\'' || ch == '"') {
          if (tokenLength > 0) {
            tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
            tokenLength = 0;
          }
          inQuote = true;
          inQuoteChar = ch;
        } else if (ch == ' ' && tokenLength > 0) {
          tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
          tokenLength = 0;
        } else if (ch == '\\')
          inEscapeSequence = true;
        else if (ch != ' ')
          token[tokenLength++] = inputBytes[i];
      }
    }
    if (inQuote)
      throw new BadArgumentException("missing terminating quote", input, input.length());
    else if (inEscapeSequence || hexChars != null)
      throw new BadArgumentException("escape sequence not complete", input, input.length());
    if (tokenLength > 0)
      tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
  }
  
  @Override
  public Iterator<String> iterator() {
    return tokens.iterator();
  }
}
