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

import org.apache.accumulo.core.clientImpl.Translator.CVSTranslator;
import org.apache.accumulo.core.clientImpl.Translator.ColumnTranslator;
import org.apache.accumulo.core.clientImpl.Translator.KeyExtentTranslator;
import org.apache.accumulo.core.clientImpl.Translator.RangeTranslator;
import org.apache.accumulo.core.clientImpl.Translator.StoredTabletFileTranslator;
import org.apache.accumulo.core.clientImpl.Translator.TCVSTranslator;
import org.apache.accumulo.core.clientImpl.Translator.TKeyExtentTranslator;
import org.apache.accumulo.core.clientImpl.Translator.TRangeTranslator;
import org.apache.accumulo.core.clientImpl.Translator.TabletFileTranslator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.thrift.TRange;

public class Translators {
  public static final TKeyExtentTranslator TKET = new TKeyExtentTranslator();
  public static final TCVSTranslator TCVST = new TCVSTranslator();
  public static final TRangeTranslator TRT = new TRangeTranslator();
  public static final KeyExtentTranslator KET = new KeyExtentTranslator();
  public static final ColumnTranslator CT = new ColumnTranslator();
  public static final Translator<Range,TRange> RT = new RangeTranslator();
  public static final CVSTranslator CVST = new CVSTranslator();
  public static final TabletFileTranslator TFT = new TabletFileTranslator();
  public static final StoredTabletFileTranslator STFT = new StoredTabletFileTranslator();
}
