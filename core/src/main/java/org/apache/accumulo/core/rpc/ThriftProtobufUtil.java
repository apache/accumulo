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
package org.apache.accumulo.core.rpc;

import static java.util.stream.Collectors.toList;

import java.util.Optional;

import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.compaction.protobuf.PCompactionKind;
import org.apache.accumulo.core.compaction.protobuf.PCredentials;
import org.apache.accumulo.core.compaction.protobuf.PExternalCompactionJob;
import org.apache.accumulo.core.compaction.protobuf.PFateId;
import org.apache.accumulo.core.compaction.protobuf.PFateInstanceType;
import org.apache.accumulo.core.compaction.protobuf.PInfo;
import org.apache.accumulo.core.compaction.protobuf.PInputFile;
import org.apache.accumulo.core.compaction.protobuf.PIteratorConfig;
import org.apache.accumulo.core.compaction.protobuf.PIteratorSetting;
import org.apache.accumulo.core.compaction.protobuf.PKeyExtent;
import org.apache.accumulo.core.compaction.protobuf.PNextCompactionJob;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.manager.thrift.TFateId;
import org.apache.accumulo.core.manager.thrift.TFateInstanceType;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TIteratorSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * This is a utility class for converting between the equivalent Thrift and Protobuf versions of the
 * objects needed using the GRPC service.
 *
 * TODO: This code may eventually be able to be removed
 *
 * This class is designed mostly just for this prototype/proof of concept and would probably not be
 * needed if we plan to keep using protocol buffers and GRPC going forward. The Thrift API is used
 * by the rest of the compaction code in the Compactor and Compaction Coordinator, so doing a
 * conversion made it easy to isolate the GRPC changes to the one getCompactionJob() RPC call
 * without having to impact the rest of the code for this prototype.
 *
 * Ideally, if we decide to go with GRPC and keep protocol buffers, then we would want to remove the
 * Thrift conversion entirely for performance reasons and just go between the native objects and
 * protocol buffer versions.
 *
 * Thrift and proto3 are pretty similar with their defintions and usage but there are a couple key
 * differences to note:
 *
 * 1) Thrift uses constructors/setters for creation objects and protocol buffers uses the builder
 * pattern
 *
 * 2) There are no nulls allowed in proto3. All values have a default value (empty string, etc)
 * depending on the type. There are also no truly optional fields (everything is initialized),
 * however, the latest versions of proto3 adds back support for the "optional" keyword. This allows
 * us to replicate null because adding the keyword generates hasX() methods.
 *
 * Instead of checking if a field is null, we can check if the field was set by calling hasX(). For
 * example, below with TCredentials, we would check if the instanceId was set by checking for null
 * but for the proto version of PCredentials we can call credentials.hasInstanceId() and check that
 * boolean. For more info see https://protobuf.dev/programming-guides/proto3/
 *
 */
public class ThriftProtobufUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftProtobufUtil.class);

  public static PInfo convert(TInfo tinfo) {
    var builder = PInfo.newBuilder();
    Optional.ofNullable(tinfo.getHeaders()).ifPresent(builder::putAllHeaders);
    return builder.build();
  }

  public static TInfo convert(PInfo pinfo) {
    return new TInfo(pinfo.getHeadersMap());
  }

  public static PCredentials convert(TCredentials credentials) {
    var builder = PCredentials.newBuilder();
    Optional.ofNullable(credentials.getPrincipal()).ifPresent(builder::setPrincipal);
    Optional.ofNullable(credentials.getTokenClassName()).ifPresent(builder::setTokenClassName);
    Optional.ofNullable(credentials.getToken())
        .ifPresent(token -> builder.setToken(ByteString.copyFrom(token)));
    Optional.ofNullable(credentials.getInstanceId()).ifPresent(builder::setInstanceId);
    return builder.build();
  }

  public static TCredentials convert(PCredentials credentials) {
    var principal = credentials.hasPrincipal() ? credentials.getPrincipal() : null;
    var tokenClassName = credentials.hasTokenClassName() ? credentials.getTokenClassName() : null;
    var token = credentials.hasToken() ? credentials.getToken().asReadOnlyByteBuffer() : null;
    var instanceId = credentials.hasInstanceId() ? credentials.getInstanceId() : null;
    return new TCredentials(principal, tokenClassName, token, instanceId);
  }

  public static TKeyExtent convert(PKeyExtent extent) {
    TKeyExtent tExtent = new TKeyExtent();
    tExtent.setTable(extent.getTable().asReadOnlyByteBuffer());
    if (extent.hasPrevEndRow()) {
      tExtent.setPrevEndRow(extent.getPrevEndRow().asReadOnlyByteBuffer());
    }
    if (extent.hasEndRow()) {
      tExtent.setEndRow(extent.getEndRow().asReadOnlyByteBuffer());
    }
    return tExtent;
  }

  public static PKeyExtent convert(TKeyExtent extent) {
    var builder = PKeyExtent.newBuilder().setTable(ByteString.copyFrom(extent.getTable()));
    Optional.ofNullable(extent.getPrevEndRow())
        .ifPresent(prevEndRow -> builder.setPrevEndRow(ByteString.copyFrom(prevEndRow)));
    Optional.ofNullable(extent.getEndRow())
        .ifPresent(endRow -> builder.setEndRow(ByteString.copyFrom(endRow)));
    return builder.build();
  }

  public static InputFile convert(PInputFile inputFile) {
    InputFile tInputFile = new InputFile();
    tInputFile.setMetadataFileEntry(inputFile.getMetadataFileEntry());
    tInputFile.setSize(inputFile.getSize());
    tInputFile.setEntries(inputFile.getEntries());
    tInputFile.setTimestamp(inputFile.getTimestamp());
    return tInputFile;
  }

  public static PInputFile convert(InputFile inputFile) {
    return PInputFile.newBuilder().setMetadataFileEntry(inputFile.getMetadataFileEntry())
        .setSize(inputFile.getSize()).setEntries(inputFile.getEntries())
        .setTimestamp(inputFile.getTimestamp()).build();
  }

  public static TIteratorSetting convert(PIteratorSetting iteratorSetting) {
    TIteratorSetting tIterSetting = new TIteratorSetting();
    tIterSetting.setPriority(iteratorSetting.getPriority());
    tIterSetting.setName(iteratorSetting.getName());
    tIterSetting.setIteratorClass(iteratorSetting.getIteratorClass());
    tIterSetting.setProperties(iteratorSetting.getPropertiesMap());
    return tIterSetting;
  }

  public static PIteratorSetting convert(TIteratorSetting iteratorSetting) {
    return PIteratorSetting.newBuilder().setPriority(iteratorSetting.getPriority())
        .setName(iteratorSetting.getName()).setIteratorClass(iteratorSetting.getIteratorClass())
        .putAllProperties(iteratorSetting.getProperties()).build();
  }

  public static IteratorConfig convert(PIteratorConfig setting) {
    IteratorConfig tIterConfig = new IteratorConfig();
    tIterConfig.setIterators(
        setting.getIteratorsList().stream().map(ThriftProtobufUtil::convert).collect(toList()));
    return tIterConfig;
  }

  public static PIteratorConfig convert(IteratorConfig setting) {
    return PIteratorConfig.newBuilder()
        .addAllIterators(
            setting.getIterators().stream().map(ThriftProtobufUtil::convert).collect(toList()))
        .build();
  }

  public static TCompactionKind convert(PCompactionKind kind) {
    switch (kind) {
      case CK_SYSTEM:
        return TCompactionKind.SYSTEM;
      case CK_USER:
        return TCompactionKind.USER;
      case CK_UNKNOWN:
        return null;
      default:
        throw new IllegalArgumentException("Unexpected PCompactionKind: " + kind);
    }
  }

  public static PCompactionKind convert(TCompactionKind kind) {
    if (kind == null) {
      return PCompactionKind.CK_UNKNOWN;
    }
    switch (kind) {
      case SYSTEM:
        return PCompactionKind.CK_SYSTEM;
      case USER:
        return PCompactionKind.CK_USER;
      default:
        throw new IllegalArgumentException("Unexpected TCompactionKind: " + kind);
    }
  }

  public static TFateId convert(PFateId fateId) {
    TFateInstanceType type;

    switch (fateId.getType()) {
      case FI_META:
        type = TFateInstanceType.META;
        break;
      case FI_USER:
        type = TFateInstanceType.USER;
        break;
      case FI_UNKNOWN:
        return null;
      default:
        throw new IllegalArgumentException("Unexpected TFateInstanceType: " + fateId.getType());
    }

    return new TFateId(type, fateId.getTxUUIDStr());
  }

  public static PFateId convert(TFateId fateId) {
    PFateInstanceType type;

    switch (fateId.getType()) {
      case META:
        type = PFateInstanceType.FI_META;
        break;
      case USER:
        type = PFateInstanceType.FI_USER;
        break;
      default:
        throw new IllegalArgumentException("Unexpected TFateInstanceType: " + fateId.getType());
    }

    return PFateId.newBuilder().setTypeValue(type.getNumber()).setTxUUIDStr(fateId.getTxUUIDStr())
        .build();
  }

  public static PExternalCompactionJob convert(TExternalCompactionJob extJob) {
    var builder = PExternalCompactionJob.newBuilder();

    Optional.ofNullable(extJob.getExternalCompactionId())
        .ifPresent(builder::setExternalCompactionId);
    Optional.ofNullable(extJob.getExtent()).map(ThriftProtobufUtil::convert)
        .ifPresent(builder::setExtent);
    Optional.ofNullable(extJob.getFiles())
        .map(files -> files.stream().map(ThriftProtobufUtil::convert).collect(toList()))
        .ifPresent(builder::addAllFiles);
    Optional.ofNullable(extJob.getIteratorSettings()).map(ThriftProtobufUtil::convert)
        .ifPresent(builder::setIteratorSettings);
    Optional.ofNullable(extJob.getOutputFile()).ifPresent(builder::setOutputFile);
    builder.setPropagateDeletes(extJob.isPropagateDeletes());
    Optional.ofNullable(extJob.getKind()).map(ThriftProtobufUtil::convert)
        .ifPresent(builder::setKind);
    Optional.ofNullable(extJob.getFateId()).map(ThriftProtobufUtil::convert)
        .ifPresent(builder::setFateId);
    Optional.ofNullable(extJob.getOverrides()).ifPresent(builder::putAllOverrides);

    PExternalCompactionJob job = builder.build();

    if (extJob.getExternalCompactionId() != null) {
      LOG.debug("TExternalCompactionJob: {}", extJob);
      LOG.debug("PExternalCompactionJob: {}", job);
    }

    return job;
  }

  public static TExternalCompactionJob convert(PExternalCompactionJob extJob) {
    TExternalCompactionJob tExtJob = new TExternalCompactionJob();

    var optExtJob = Optional.of(extJob);
    optExtJob.filter(PExternalCompactionJob::hasExternalCompactionId)
        .map(PExternalCompactionJob::getExternalCompactionId)
        .ifPresent(tExtJob::setExternalCompactionId);
    optExtJob.filter(PExternalCompactionJob::hasExtent).map(job -> convert(job.getExtent()))
        .ifPresent(tExtJob::setExtent);
    if (!extJob.getFilesList().isEmpty()) {
      tExtJob.setFiles(
          extJob.getFilesList().stream().map(ThriftProtobufUtil::convert).collect(toList()));
    }
    optExtJob.filter(PExternalCompactionJob::hasIteratorSettings)
        .map(job -> convert(job.getIteratorSettings())).ifPresent(tExtJob::setIteratorSettings);
    optExtJob.filter(PExternalCompactionJob::hasOutputFile)
        .map(PExternalCompactionJob::getOutputFile).ifPresent(tExtJob::setOutputFile);
    tExtJob.setPropagateDeletes(extJob.getPropagateDeletes());
    optExtJob.filter(PExternalCompactionJob::hasKind).map(job -> convert(job.getKind()))
        .ifPresent(tExtJob::setKind);
    optExtJob.filter(PExternalCompactionJob::hasFateId).map(job -> convert(job.getFateId()))
        .ifPresent(tExtJob::setFateId);
    tExtJob.setOverrides(extJob.getOverridesMap());

    if (!extJob.getExternalCompactionId().isEmpty()) {
      LOG.trace("PExternalCompactionJob: {}", extJob);
      LOG.trace("TExternalCompactionJob: {}", tExtJob);
    }

    return tExtJob;
  }

  public static TNextCompactionJob convert(PNextCompactionJob nextJob) {
    var job = new TNextCompactionJob();
    job.setJob(convert(nextJob.getJob()));
    job.setCompactorCount(nextJob.getCompactorCount());
    return job;
  }

  public static PNextCompactionJob convert(TNextCompactionJob nextJob) {
    return PNextCompactionJob.newBuilder().setCompactorCount(nextJob.getCompactorCount())
        .setJob(convert(nextJob.getJob())).build();
  }
}
