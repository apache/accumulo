/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * A factory to allow applications to deal with inconsistencies between MapReduce Context Objects API between hadoop-0.20 and later versions. This code is based
 * on org.apache.hadoop.mapreduce.ContextFactory in hadoop-mapred-0.22.0.
 */
public class ContextFactory {
  
  private static final Constructor<?> JOB_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> TASK_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> TASK_ID_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONTEXT_CONSTRUCTOR;
  private static final Constructor<?> MAP_CONTEXT_IMPL_CONSTRUCTOR;
  private static final Class<?> TASK_TYPE_CLASS;
  private static final boolean useV21;
  
  static {
    boolean v21 = true;
    final String PACKAGE = "org.apache.hadoop.mapreduce";
    try {
      Class.forName(PACKAGE + ".task.JobContextImpl");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }
    useV21 = v21;
    Class<?> jobContextCls;
    Class<?> taskContextCls;
    Class<?> mapCls;
    Class<?> mapContextCls;
    Class<?> innerMapContextCls;
    try {
      if (v21) {
        jobContextCls = Class.forName(PACKAGE + ".task.JobContextImpl");
        taskContextCls = Class.forName(PACKAGE + ".task.TaskAttemptContextImpl");
        TASK_TYPE_CLASS = Class.forName(PACKAGE + ".TaskType");
        mapContextCls = Class.forName(PACKAGE + ".task.MapContextImpl");
        mapCls = Class.forName(PACKAGE + ".lib.map.WrappedMapper");
        innerMapContextCls = Class.forName(PACKAGE + ".lib.map.WrappedMapper$Context");
      } else {
        jobContextCls = Class.forName(PACKAGE + ".JobContext");
        taskContextCls = Class.forName(PACKAGE + ".TaskAttemptContext");
        TASK_TYPE_CLASS = null;
        mapContextCls = Class.forName(PACKAGE + ".MapContext");
        mapCls = Class.forName(PACKAGE + ".Mapper");
        innerMapContextCls = Class.forName(PACKAGE + ".Mapper$Context");
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    }
    try {
      JOB_CONTEXT_CONSTRUCTOR = jobContextCls.getConstructor(Configuration.class, JobID.class);
      JOB_CONTEXT_CONSTRUCTOR.setAccessible(true);
      TASK_CONTEXT_CONSTRUCTOR = taskContextCls.getConstructor(Configuration.class, TaskAttemptID.class);
      TASK_CONTEXT_CONSTRUCTOR.setAccessible(true);
      if (useV21) {
        TASK_ID_CONSTRUCTOR = TaskAttemptID.class.getConstructor(String.class, int.class, TASK_TYPE_CLASS, int.class, int.class);
        TASK_ID_CONSTRUCTOR.setAccessible(true);
        MAP_CONSTRUCTOR = mapCls.getConstructor();
        MAP_CONTEXT_CONSTRUCTOR = innerMapContextCls.getConstructor(mapCls, MapContext.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR = mapContextCls.getDeclaredConstructor(Configuration.class, TaskAttemptID.class, RecordReader.class, RecordWriter.class,
            OutputCommitter.class, StatusReporter.class, InputSplit.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR.setAccessible(true);
      } else {
        TASK_ID_CONSTRUCTOR = TaskAttemptID.class.getConstructor(String.class, int.class, boolean.class, int.class, int.class);
        TASK_ID_CONSTRUCTOR.setAccessible(true);
        MAP_CONSTRUCTOR = null;
        MAP_CONTEXT_CONSTRUCTOR = innerMapContextCls.getConstructor(mapCls, Configuration.class, TaskAttemptID.class, RecordReader.class, RecordWriter.class,
            OutputCommitter.class, StatusReporter.class, InputSplit.class);
        MAP_CONTEXT_IMPL_CONSTRUCTOR = null;
      }
      MAP_CONTEXT_CONSTRUCTOR.setAccessible(true);
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Can't run constructor ", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    }
  }
  
  public static JobContext createJobContext() {
    return createJobContext(new Configuration());
  }
  
  public static JobContext createJobContext(Configuration conf) {
    try {
      return (JobContext) JOB_CONTEXT_CONSTRUCTOR.newInstance(conf, new JobID("local", 0));
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't create object", e);
    }
  }
  
  public static TaskAttemptContext createTaskAttemptContext(JobContext job) {
    return createTaskAttemptContext(job.getConfiguration());
  }
  
  public static TaskAttemptContext createTaskAttemptContext(Configuration conf) {
    try {
      if (useV21)
        return (TaskAttemptContext) TASK_CONTEXT_CONSTRUCTOR.newInstance(conf,
            TASK_ID_CONSTRUCTOR.newInstance("local", 0, TASK_TYPE_CLASS.getEnumConstants()[0], 0, 0));
      else
        return (TaskAttemptContext) TASK_CONTEXT_CONSTRUCTOR.newInstance(conf, TASK_ID_CONSTRUCTOR.newInstance("local", 0, true, 0, 0));
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't create object", e);
    }
  }
  
  public static <K1,V1,K2,V2> Mapper<K1,V1,K2,V2>.Context createMapContext(Mapper<K1,V1,K2,V2> m, TaskAttemptContext tac, RecordReader<K1,V1> reader,
      RecordWriter<K2,V2> writer, InputSplit split) {
    return createMapContext(m, tac, reader, writer, null, null, split);
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <K1,V1,K2,V2> Mapper<K1,V1,K2,V2>.Context createMapContext(Mapper<K1,V1,K2,V2> m, TaskAttemptContext tac, RecordReader<K1,V1> reader,
      RecordWriter<K2,V2> writer, OutputCommitter committer, StatusReporter reporter, InputSplit split) {
    try {
      if (useV21) {
        Object basis = MAP_CONTEXT_IMPL_CONSTRUCTOR.newInstance(tac.getConfiguration(), tac.getTaskAttemptID(), reader, writer, committer, reporter, split);
        return (Mapper.Context) MAP_CONTEXT_CONSTRUCTOR.newInstance((Mapper<K1,V1,K2,V2>) MAP_CONSTRUCTOR.newInstance(), basis);
      } else {
        return (Mapper.Context) MAP_CONTEXT_CONSTRUCTOR.newInstance(m, tac.getConfiguration(), tac.getTaskAttemptID(), reader, writer, committer, reporter,
            split);
      }
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't create object", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't create object", e);
    }
  }
}
