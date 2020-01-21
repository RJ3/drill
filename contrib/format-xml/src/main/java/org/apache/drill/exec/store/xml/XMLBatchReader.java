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

import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Stack;

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

  private Stack<String> nestedFieldNameStack;

  private Stack<TupleWriter> rowWriterStack;

  private XMLDataVector nested_data2;

  private boolean flattenAttributes;

  private boolean flatten;

  private boolean inNested = false;

  private int columnIndex = 0;

  private int dataLevel = 3; // TODO set this in the config


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
    nestedFieldNameStack = new Stack<>();
    rowWriterStack = new Stack<>();
  }


  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    loader = negotiator.build();
    rowWriter = loader.writer();
    openFile(negotiator);
    return true;
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
      try {
        if (!nextLine(rowWriter)) {
          return false;
        }
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

  /**
   *
   * @param rowWriter The rowWriter for the main
   * @return True if there is more data, false if not.
   * @throws Exception
   */
  private boolean nextLine(RowSetLoader rowWriter) throws Exception {
    if (!XMLReader.hasNext()) {
      return false;
    }
    XMLEvent currentEvent;
    int lastElementType = -1;
    int loopCounter = 0;
    String currentFieldName = "";
    String lastFieldName;
    String fieldValue = "";
    String fieldPrefix = "";
    int currentNestingLevel = 0;
    int dataLevel = 2;  // TODO Set this from the config
    boolean rowStarted = false;

    nested_data2 = new XMLDataVector();

    /*
     * This loop iterates over XML tags.  Depending on the tag content, the loop will take different actions.
     */
    while (XMLReader.hasNext()) {
      currentEvent = XMLReader.nextEvent();

      // Skip empty events
      if (currentEvent.toString().trim().isEmpty()) {
        continue;
      }

      if (loopCounter == 0) {
        lastElementType = currentEvent.getEventType();
      }

      switch (currentEvent.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          StartElement startElement = currentEvent.asStartElement();
          currentNestingLevel++;

          // Get the field name
          lastFieldName = currentFieldName;
          currentFieldName = startElement.getName().getLocalPart();

          if (lastElementType == XMLStreamConstants.START_ELEMENT && currentNestingLevel > dataLevel + 2) {

            /*nestedFieldNameStack.push(lastFieldName);
            logger.debug("Pushing: " + lastFieldName);

            // Add currentRowWriter to Stack
            rowWriterStack.push(rowWriter);

            // Create new schema for new map
            SchemaBuilder innerSchema = new SchemaBuilder();

            // TODO The name needs to be the last name, not the current name...
            logger.debug("Adding map for {}.", lastFieldName);

            MapBuilder mapBuilder = innerSchema.addMap(lastFieldName);
            TupleMetadata finalInnerSchema = mapBuilder.resumeSchema().buildSchema();

            int index = rowWriter.tupleSchema().index(currentFieldName);
            if (index == -1) {
              index = rowWriter.addColumn(finalInnerSchema.column(currentFieldName));
            }

            TupleWriter listWriter = rowWriter.column(index).tuple();*/
          }

          // Start the row
          if (currentNestingLevel > dataLevel && !rowStarted) {
            rowWriter.start();
            rowStarted = true;
            logger.debug("Starting new row");
          }

          // Write attributes
          int attributeCount = Iterators.size(startElement.getAttributes());
          Iterator<Attribute> attributes = startElement.getAttributes();

          //TODO Add Support for attributes on nested fields
          while (attributes.hasNext()) {
            Attribute attrib = attributes.next();
            if (flattenAttributes) {
              String attributeFieldName = currentFieldName + "_" + attrib.getName();
              writeStringColumn(rowWriter, attributeFieldName, attrib.getValue());
            } else {
              // Create a map of attributes
              writeAttributes(rowWriter, currentFieldName, attributes);
            }
          }

          lastElementType = currentEvent.getEventType();
          break;

        case XMLStreamConstants.CHARACTERS:
          // Get the field value
          Characters characters = currentEvent.asCharacters();
          fieldValue = characters.getData().trim();
          break;

        case XMLStreamConstants.END_ELEMENT:
          currentNestingLevel--;
          if (lastElementType == XMLStreamConstants.END_ELEMENT) {
            logger.debug("Reducing nesting level to {}", currentNestingLevel);
          }

          // Case to close a row
          if (currentNestingLevel < dataLevel && rowStarted) {
            rowWriter.save();
            rowStarted = false;
            logger.debug("Ending row");
          } else {
            // Case to write the element
            writeStringColumn(rowWriter, currentFieldName, fieldValue);
          }

          lastElementType = currentEvent.getEventType();
          break;
      } // End Switch Statement

      loopCounter++;
    } // End loop
    return true;
  }

  @Override
  public void close() {
    if (fsStream != null) {
      try {
        fsStream.close();
      } catch (IOException e) {
        logger.warn("Error when closing resource: {}", e.getMessage());
      }
      fsStream = null;
    }

    if (XMLReader != null) {
      try {
        XMLReader.close();
      } catch (XMLStreamException e) {
        logger.warn("Error when closing XML stream", e.getMessage());
      }
      XMLReader = null;
    }
  }

  /**
   * Helper function which writes attributes of an XML element.
   * @param rowWriter The rowWriter
   * @param currentFieldName The current field name
   * @param attributes An iterator of Attribute objects
   */
  private void writeAttributes(TupleWriter rowWriter, String currentFieldName, Iterator<Attribute> attributes) {
    String attributeFieldName = currentFieldName + "_" + "attributes";

    int index = rowWriter.tupleSchema().index(attributeFieldName);
    if (index == -1) {
      index = rowWriter
        .addColumn(SchemaBuilder.columnSchema(attributeFieldName, TypeProtos.MinorType.MAP, TypeProtos.DataMode.REQUIRED));
    }
    TupleWriter mapWriter = rowWriter.tuple(index);

    while (attributes.hasNext()) {
      Attribute currentAttribute = attributes.next();
      String key = currentAttribute.getName().toString();
      writeStringColumn(mapWriter, key, currentAttribute.getValue());
    }
  }

  private void writeStringColumn(TupleWriter rowWriter, String name, String value) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setString(value);
  }

  /**
   * Generates a nested field name by combining a field prefix to the current field name.
   * @param prefix The prefix to be added to the field name.
   * @param field The field name
   * @return the prefix, followed by an underscore and the fieldname.
   */
  private String addField(String prefix, String field) {
    return prefix + "_" + field;
  }

  /**
   * Returns the field name from nested field names
   * @param fieldName The nested field name
   * @return The field name
   */
  private String removeField(String fieldName) {
    String[] components = fieldName.split("_");
    StringBuilder newField = new StringBuilder();
    for (int i = 0; i < components.length - 1; i++) {
      if (i > 0) {
        newField.append("_").append(components[i]);
      } else {
        newField = new StringBuilder(components[i]);
      }
    }
    return newField.toString();
  }
}
