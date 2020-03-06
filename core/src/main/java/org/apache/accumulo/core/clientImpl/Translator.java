/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TConstraintViolationSummary;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;

public abstract class Translator<IT,OT> {

  public abstract OT translate(IT input);

  public static class TKeyExtentTranslator extends Translator<TKeyExtent,KeyExtent> {
    @Override
    public KeyExtent translate(TKeyExtent input) {
      return new KeyExtent(input);
    }

  }

  public static class KeyExtentTranslator extends Translator<KeyExtent,TKeyExtent> {
    @Override
    public TKeyExtent translate(KeyExtent input) {
      return input.toThrift();
    }
  }

  public static class TCVSTranslator
      extends Translator<TConstraintViolationSummary,ConstraintViolationSummary> {
    @Override
    public ConstraintViolationSummary translate(TConstraintViolationSummary input) {
      return new ConstraintViolationSummary(input);
    }
  }

  public static class CVSTranslator
      extends Translator<ConstraintViolationSummary,TConstraintViolationSummary> {
    @Override
    public TConstraintViolationSummary translate(ConstraintViolationSummary input) {
      return input.toThrift();
    }
  }

  public static class TColumnTranslator extends Translator<TColumn,Column> {
    @Override
    public Column translate(TColumn input) {
      return new Column(input);
    }
  }

  public static class ColumnTranslator extends Translator<Column,TColumn> {
    @Override
    public TColumn translate(Column input) {
      return input.toThrift();
    }
  }

  public static class TRangeTranslator extends Translator<TRange,Range> {

    @Override
    public Range translate(TRange input) {
      return new Range(input);
    }

  }

  public static class RangeTranslator extends Translator<Range,TRange> {
    @Override
    public TRange translate(Range input) {
      return input.toThrift();
    }
  }

  public static class TabletFileTranslator extends Translator<TabletFile,String> {
    @Override
    public String translate(TabletFile input) {
      return input.getPathStr();
    }
  }

  public static class StoredTabletFileTranslator extends Translator<StoredTabletFile,String> {
    @Override
    public String translate(StoredTabletFile input) {
      return input.getPathStr();
    }
  }

  public static class ListTranslator<IT,OT> extends Translator<List<IT>,List<OT>> {

    private Translator<IT,OT> translator;

    public ListTranslator(Translator<IT,OT> translator) {
      this.translator = translator;
    }

    @Override
    public List<OT> translate(List<IT> input) {
      return translate(input, this.translator);
    }

  }

  public static <IKT,OKT,T> Map<OKT,T> translate(Map<IKT,T> input,
      Translator<IKT,OKT> keyTranslator) {
    HashMap<OKT,T> output = new HashMap<>();

    for (Entry<IKT,T> entry : input.entrySet())
      output.put(keyTranslator.translate(entry.getKey()), entry.getValue());

    return output;
  }

  public static <IKT,OKT,IVT,OVT> Map<OKT,OVT> translate(Map<IKT,IVT> input,
      Translator<IKT,OKT> keyTranslator, Translator<IVT,OVT> valueTranslator) {
    HashMap<OKT,OVT> output = new HashMap<>();

    for (Entry<IKT,IVT> entry : input.entrySet())
      output.put(keyTranslator.translate(entry.getKey()),
          valueTranslator.translate(entry.getValue()));

    return output;
  }

  public static <IT,OT> List<OT> translate(Collection<IT> input, Translator<IT,OT> translator) {
    ArrayList<OT> output = new ArrayList<>(input.size());

    for (IT in : input)
      output.add(translator.translate(in));

    return output;
  }
}
