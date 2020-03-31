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

import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads httpd logs lines terminated with a newline character.
 */
public class HttpdLogRecordReader extends AbstractRecordReader {
  private static final Logger logger = LoggerFactory.getLogger(HttpdLogRecordReader.class);

  private static final int VECTOR_MEMORY_ALLOCATION = 4095;


  private final DrillFileSystem fs;
  private final FileWork work;
  private final FragmentContext fragmentContext;
  private BaseWriter.ComplexWriter writer;
  private HttpdParser parser;
  private LineRecordReader lineReader;
  private LongWritable lineNumber;
  private HttpdLogFormatPlugin plugin;

  public HttpdLogRecordReader(final FragmentContext context, final DrillFileSystem fs, final FileWork work, final List<SchemaPath> columns, HttpdLogFormatPlugin plugin) {
    this.fs = fs;
    this.work = work;
    this.fragmentContext = context;
    this.plugin = plugin;
    setColumns(columns);
  }

  /**
   * The query fields passed in are formatted in a way that Drill requires.
   * Those must be cleaned up to work with the parser.
   *
   * @return Map with Drill field names as a key and Parser Field names as a
   *         value
   */
  private Map<String, String> makeParserFields() {
    Map<String, String> fieldMapping = new HashMap<>();
    for (final SchemaPath sp : getColumns()) {
      String drillField = sp.getRootSegment().getPath();
      try {
        String parserField = HttpdParser.parserFormattedFieldName(drillField);
        fieldMapping.put(drillField, parserField);
      } catch (Exception e) {
        logger.info("Putting field: {} into map", drillField, e);
      }
    }
    return fieldMapping;
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) {
    try {
      /*
       * Extract the list of field names for the parser to use if it is NOT a star query. If it is a star query just
       * pass through an empty map, because the parser is going to have to build all possibilities.
       */
      final Map<String, String> fieldMapping = !isStarQuery() ? makeParserFields() : null;
      writer = new VectorContainerWriter(output);

      parser = new HttpdParser(writer.rootAsMap(), context.getManagedBuffer(),
        plugin.getConfig().getLogFormat(),
        plugin.getConfig().getTimestampFormat(),
        fieldMapping);

      final Path path = fs.makeQualified(work.getPath());
      FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
      TextInputFormat inputFormat = new TextInputFormat();
      JobConf job = new JobConf(fs.getConf());
      job.setInt("io.file.buffer.size", fragmentContext.getConfig().getInt(ExecConstants.TEXT_LINE_READER_BUFFER_SIZE));
      job.setInputFormat(inputFormat.getClass());
      lineReader = (LineRecordReader) inputFormat.getRecordReader(split, job, Reporter.NULL);
      lineNumber = lineReader.createKey();
    } catch (NoSuchMethodException | MissingDissectorsException | InvalidDissectorException e) {
      throw handleAndGenerate("Failure creating HttpdParser", e);
    } catch (IOException e) {
      throw handleAndGenerate("Failure creating HttpdRecordReader", e);
    }
  }

  private RuntimeException handleAndGenerate(final String s, final Exception e) {
    throw UserException.dataReadError(e)
      .message(s + "\n%s", e.getMessage())
      .addContext("Path", work.getPath())
      .addContext("Split Start", work.getStart())
      .addContext("Split Length", work.getLength())
      .addContext("Local Line Number", lineNumber.get())
      .build(logger);
  }

  /**
   * This record reader is given a batch of records (lines) to read. Next acts upon a batch of records.
   *
   * @return Number of records in this batch.
   */
  @Override
  public int next() {
    try {
      final Text line = lineReader.createValue();

      writer.allocate();
      writer.reset();

      int recordCount = 0;
      while (recordCount < VECTOR_MEMORY_ALLOCATION && lineReader.next(lineNumber, line)) {
        writer.setPosition(recordCount);
        parser.parse(line.toString());
        recordCount++;
      }
      writer.setValueCount(recordCount);

      return recordCount;
    } catch (DissectionFailure | InvalidDissectorException | MissingDissectorsException | IOException e) {
      throw handleAndGenerate("Failure while parsing log record.", e);
    }
  }

  @Override
  public void close() {
    try {
      if (lineReader != null) {
        lineReader.close();
      }
    } catch (IOException e) {
      logger.warn("Failure while closing Httpd reader.", e);
    }
  }
}
