/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.xml;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;


public class XMLFormatPlugin extends EasyFormatPlugin<XMLFormatConfig> {

  public static final String DEFAULT_NAME = "xml";

  private XMLFormatConfig config;

  private static class XMLReaderFactory extends FileScanFramework.FileReaderFactory {
    private final XMLBatchReader.XMLReaderConfig readerConfig;

    public XMLReaderFactory(XMLBatchReader.XMLReaderConfig config) {
      readerConfig = config;
    }

    @Override
    public ManagedReader<? extends FileScanFramework.FileSchemaNegotiator> newReader() {
      return new XMLBatchReader(readerConfig);
    }
  }

  public XMLFormatPlugin(String name, DrillbitContext context,
                         Configuration fsConf, StoragePluginConfig storageConfig,
                         XMLFormatConfig formatConfig) {
    super(name, easyConfig(fsConf, formatConfig), context, storageConfig, formatConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, XMLFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = false;
    config.blockSplittable = true;
    config.compressible = true;
    config.supportsProjectPushdown = true;
    config.extensions = Lists.newArrayList(pluginConfig.getExtensions());
    config.fsConf = fsConf;
    config.defaultName = DEFAULT_NAME;
    config.readerOperatorType = UserBitShared.CoreOperatorType.REGEX_SUB_SCAN_VALUE;  // TODO Fix this Later
    config.useEnhancedScan = true;
    return config;
  }

  @Override
  public ManagedReader<? extends FileScanFramework.FileSchemaNegotiator> newBatchReader(
    EasySubScan scan, OptionManager options) throws ExecutionSetupException {
    return new XMLBatchReader(formatConfig.getReaderConfig(this));
  }

  @Override
  protected FileScanBuilder frameworkBuilder(OptionManager options, EasySubScan scan) throws ExecutionSetupException {
    FileScanBuilder builder = new FileScanBuilder();
    XMLBatchReader.XMLReaderConfig readerConfig = new XMLBatchReader.XMLReaderConfig(this);

    builder.setReaderFactory(new XMLReaderFactory(readerConfig));

    initScanBuilder(builder, scan);
    builder.setNullType(Types.optional(TypeProtos.MinorType.VARCHAR));
    return builder;
  }
}
