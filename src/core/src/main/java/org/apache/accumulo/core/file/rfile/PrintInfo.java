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
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PrintInfo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
        FileSystem fs = FileUtil.getFileSystem(conf, AccumuloConfiguration.getSiteConfiguration());
        
        Options opts = new Options();
        Option dumpKeys = new Option("d", "dump", false, "dump the key/value pairs");
        opts.addOption(dumpKeys);
        
        CommandLine commandLine = new BasicParser().parse(opts, args);
        
        for (String arg : commandLine.getArgs()) {
            
            Path path = new Path(arg);
            CachableBlockFile.Reader _rdr = new CachableBlockFile.Reader(fs, path, conf, null, null);
            Reader iter = new RFile.Reader(_rdr);
            
            iter.printInfo();
            System.out.println();
            org.apache.accumulo.core.file.rfile.bcfile.PrintInfo.main(new String[] {arg});
            
            if (commandLine.hasOption(dumpKeys.getOpt())) {
                iter.seek(new Range((Key) null, (Key) null), new ArrayList<ByteSequence>(), false);
                while (iter.hasTop()) {
                    Key key = iter.getTopKey();
                    Value value = iter.getTopValue();
                    System.out.println(key + " -> " + value);
                    iter.next();
                }
            }
            
            iter.close();
        }
    }
}
