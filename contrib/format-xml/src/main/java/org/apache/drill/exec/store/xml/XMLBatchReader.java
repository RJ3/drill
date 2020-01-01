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

import com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class XMLBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(XMLBatchReader.class);

  private XMLEventReader XMLReader;

  private final XMLReaderConfig readerConfig;

  private FileSplit split;

  private XMLFormatConfig formatConfig;

  private int nestingLevel;

  private InputStream fsStream;

  private ResultSetLoader loader;

  private RowSetLoader rowWriter;

  private boolean flattenAttributes;

  private boolean flatten;


  static class XMLReaderConfig {
    final XMLFormatPlugin plugin;
    final boolean flatten;
    final boolean flattenAttributes;

    XMLReaderConfig(XMLFormatPlugin plugin) {
      this.plugin = plugin;
      flatten = plugin.getConfig().flatten;
      flattenAttributes = plugin.getConfig().flattenAttributes;
    }
  }

  public XMLBatchReader(XMLReaderConfig readerConfig) {
    this.readerConfig = readerConfig;
    this.flattenAttributes = readerConfig.flattenAttributes;
    this.flatten = readerConfig.flatten;
    nestingLevel = 0;
  }


  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    loader = negotiator.build();
    rowWriter = loader.writer();
    openFile(negotiator);


  }

  private void openFile(FileScanFramework.FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      XMLInputFactory inputFactory = XMLInputFactory.newInstance();
      XMLReader = inputFactory.createXMLEventReader(fsStream);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: {}", split.getPath().toString())
        .message(e.getMessage())
        .build(logger);
    }
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    if (!XMLReader.hasNext()) {
      return false;
    }

    XMLEvent currentEvent;
    int lastElementType = -1;
    int loopCounter = 0;
    String currentFieldName;
    String fieldValue = "";
    String fieldPrefix = "";

    /*
    This loop iterates over XML tags.  Depending on the tag content, the loop will take different actions.
     */
    while (XMLReader.hasNext()) {
      currentEvent = this.XMLReader.nextEvent();

      //Skip empty events
      if (currentEvent.toString().trim().isEmpty()) {
        continue;
      }

      if (loopCounter == 0) {
        lastElementType = currentEvent.getEventType();
      }

      switch (currentEvent.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          StartElement startElement = currentEvent.asStartElement();
          String qName = startElement.getName().getLocalPart();

          if (lastElementType == XMLStreamConstants.START_ELEMENT) {

            // TODO Only push to stack above data level
            this.nested_field_name_stack.push(currentFieldName);
            System.out.println("Pushing: " + currentFieldName);

            //nested_data_stack.push(map.map(current_field_name));

            nested_data2.setNestedFieldName(currentFieldName);
          }
          currentFieldName = startElement.getName().getLocalPart();

          int attribute_count = Iterators.size(startElement.getAttributes());

          if (!readerConfig.plugin.getConfig().flattenAttributes && attribute_count > 0) {
            attrib_map = map.map(currentFieldName + "_attribs");
            attrib_map.start();
          }

          Iterator<Attribute> attributes = startElement.getAttributes();

          //TODO Add Support for attributes on nested fields
          while (attributes.hasNext()) {
            Attribute a = attributes.next();
            if (flattenAttributes) {
              String attrib_field_name = currentFieldName + "_" + a.getName();
              byte[] bytes = a.getValue().getBytes("UTF-8");
              this.buffer.setBytes(0, bytes, 0, bytes.length);
              map.varChar(attrib_field_name).writeVarChar(0, bytes.length, buffer);

            } else {
              //Create a map of attributes
              String attrib_name = a.getName().toString();
              byte[] bytes = a.getValue().getBytes("UTF-8");
              this.buffer.setBytes(0, bytes, 0, bytes.length);
              attrib_map.varChar(attrib_name).writeVarChar(0, bytes.length, buffer);
            }

          }

          if (!flattenAttributes && attribute_count > 0) {
            attrib_map.end();
          }

          nesting_level++;
          fieldPrefix = addField(fieldPrefix, currentFieldName);

          break;

        /*
         *  This is the case in which the reader encounters data between tags.  In this case, the data is read as the
         *  field value and trailing (and leading) white space is removed.
         */
        case XMLStreamConstants.CHARACTERS:
          Characters characters = currentEvent.asCharacters();
          fieldValue = characters.getData().trim();
          break;
      }

      loopCounter++;
    }

  }

  @Override
  public void close() {
    if (fsStream != null) {
      try {
        fsStream.close();
      } catch (IOException e) {
        logger.warn("Error when closing XSSFWorkbook resource: {}", e.getMessage());
      }
      fsStream = null;
    }
  }

}
