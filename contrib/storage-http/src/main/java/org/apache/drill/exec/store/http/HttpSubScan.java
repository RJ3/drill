/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.http;


import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonTypeName("http-sub-scan")
public class HttpSubScan extends AbstractSubScan {
  private static final Logger logger = LoggerFactory.getLogger(HttpSubScan.class);

  private final HttpScanSpec tableSpec;
  private final HttpStoragePluginConfig config;
  private final List<SchemaPath> columns;

  @JsonCreator
  public HttpSubScan(
    @JsonProperty("config") HttpStoragePluginConfig config,
    @JsonProperty("tableSpec") HttpScanSpec tableSpec,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super("user-if-needed");
    this.config = config;
    this.tableSpec = tableSpec;
    this.columns = columns;
  }

  public HttpScanSpec getTableSpec() {
    return tableSpec;
  }

  public String getURL() {
    return tableSpec.getURL();
  }

  public String getFullURL() {
    return config.getConnection() + getURL();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public HttpStoragePluginConfig getConfig() {
    return config;
  }

 @Override
  public <T, X, E extends Throwable> T accept(
   PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
    throws ExecutionSetupException {
    return new HttpSubScan(config, tableSpec, columns);
  }

  @Override
  @JsonIgnore
  public int getOperatorType() {
    return CoreOperatorType.HTTP_SUB_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() +
      "httpScanSpec=" + tableSpec.toString() +
      "columns=" + columns.toString() + "]";
  }
}
