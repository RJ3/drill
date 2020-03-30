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
package org.apache.drill.exec.store.httpd;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.parquet.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@JsonTypeName(HttpdLogFormatPlugin.DEFAULT_NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class HttpdLogFormatConfig implements FormatPluginConfig {

  public static final String DEFAULT_TIME_FORMAT = "dd/MMM/yyyy:HH:mm:ss ZZ";

  public final String logFormat;

  public final String timestampFormat;

  public final List<String> extensions;

  @JsonCreator
  public HttpdLogFormatConfig(@JsonProperty("logFormat") String logFormat,
                              @JsonProperty("timestampFormat") String timestampFormat,
                              @JsonProperty("extensions") List<String> extensions ) {
    this.logFormat = logFormat;
    this.timestampFormat = Strings.isNullOrEmpty(timestampFormat) ? timestampFormat : DEFAULT_TIME_FORMAT;
    if (extensions == null) {
      this.extensions = Collections.singletonList("httpd");
    } else {
      this.extensions = extensions;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public String getLogFormat() {
    return logFormat;
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public String getTimestampFormat() {
    return timestampFormat;
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(
      new Object[]{logFormat, timestampFormat});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HttpdLogFormatConfig other = (HttpdLogFormatConfig) obj;
    return Objects.equals(logFormat, other.logFormat)
      && Objects.equals(timestampFormat, other.timestampFormat);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("logFormat", logFormat)
      .field("timestampFormat", timestampFormat)
      .toString();
  }
}
