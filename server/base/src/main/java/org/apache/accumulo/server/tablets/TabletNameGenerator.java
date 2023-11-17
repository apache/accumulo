/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.tablets;

import java.util.function.Consumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * contains code related to generating new file path for a tablet
 */
public class TabletNameGenerator {

  public static String createTabletDirectoryName(ServerContext context, Text endRow) {
    if (endRow == null) {
      return MetadataSchema.TabletsSection.ServerColumnFamily.DEFAULT_TABLET_DIR_NAME;
    } else {
      UniqueNameAllocator namer = context.getUniqueNameAllocator();
      return Constants.GENERATED_TABLET_DIRECTORY_PREFIX + namer.getNextName();
    }
  }

  public static String chooseTabletDir(ServerContext context, KeyExtent extent, String dirName,
      Consumer<String> dirCreator) {
    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(extent.tableId(), extent.endRow(), context);
    String dirUri = context.getVolumeManager().choose(chooserEnv, context.getBaseUris())
        + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + extent.tableId() + Path.SEPARATOR + dirName;
    dirCreator.accept(dirUri);
    return dirUri;
  }

  public static ReferencedTabletFile getNextDataFilename(FilePrefix prefix, ServerContext context,
      KeyExtent extent, String dirName, Consumer<String> dirCreator) {
    String extension =
        FileOperations.getNewFileExtension(context.getTableConfiguration(extent.tableId()));
    return new ReferencedTabletFile(
        new Path(chooseTabletDir(context, extent, dirName, dirCreator) + "/" + prefix.toPrefix()
            + context.getUniqueNameAllocator().getNextName() + "." + extension));
  }

  public static ReferencedTabletFile getNextDataFilenameForMajc(boolean propagateDeletes,
      ServerContext context, TabletMetadata tabletMetadata, Consumer<String> dirCreator,
      ExternalCompactionId ecid) {
    String tmpFileName = getNextDataFilename(
        !propagateDeletes ? FilePrefix.MAJOR_COMPACTION_ALL_FILES : FilePrefix.MAJOR_COMPACTION,
        context, tabletMetadata.getExtent(), tabletMetadata.getDirName(), dirCreator).insert()
        .getMetadataPath() + "_tmp_" + ecid.canonical();
    return new ReferencedTabletFile(new Path(tmpFileName));
  }

  public static ReferencedTabletFile computeCompactionFileDest(ReferencedTabletFile tmpFile) {
    String newFilePath = tmpFile.getNormalizedPathStr();
    int idx = newFilePath.indexOf("_tmp");
    if (idx > 0) {
      newFilePath = newFilePath.substring(0, idx);
    } else {
      throw new IllegalArgumentException("Expected compaction tmp file "
          + tmpFile.getNormalizedPathStr() + " to have suffix starting with '_tmp'");
    }
    return new ReferencedTabletFile(new Path(newFilePath));
  }
}
