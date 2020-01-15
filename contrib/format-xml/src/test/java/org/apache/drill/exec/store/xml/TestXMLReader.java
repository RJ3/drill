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


import org.apache.drill.categories.RowSetTests;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.dfs.ZipCodec;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;

@Category(RowSetTests.class)
public class TestXMLReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    XMLFormatConfig formatConfig = new XMLFormatConfig();
    formatConfig.flatten = true;
    formatConfig.flattenAttributes =  false;
    cluster.defineFormat("cp", "xml", formatConfig);

    // Needed for compressed file unit test
    dirTestWatcher.copyResourceToRoot(Paths.get("xml/"));
  }


  @Test
  public void testWildcard() throws Exception {
    String sql = "SELECT * FROM cp.`xml/books3a.xml`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    results.print();

    assertNotNull(results);
    /*TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("author", TypeProtos.MinorType.VARCHAR)
      .addNullable("title", TypeProtos.MinorType.VARCHAR)
      .addNullable("day", TypeProtos.MinorType.INT)
      .buildSchema();
    RowSet expected = client.rowSetBuilder(expectedSchema).addRow(2017, 12, 17).addRow(2017, 12, 18).addRow(2017, 12, 19).build();
    RowSetUtilities.verify(expected, results);*/
  }

  private void generateCompressedFile(String fileName, String codecName, String outFileName) throws IOException {
    FileSystem fs = ExecTest.getLocalFileSystem();
    Configuration conf = fs.getConf();
    conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY, ZipCodec.class.getCanonicalName());
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    CompressionCodec codec = factory.getCodecByName(codecName);
    assertNotNull(codecName + " is not found", codec);

    Path outFile = new Path(dirTestWatcher.getRootDir().getAbsolutePath(), outFileName);
    Path inFile = new Path(dirTestWatcher.getRootDir().getAbsolutePath(), fileName);

    try (InputStream inputStream = new FileInputStream(inFile.toUri().toString());
         OutputStream outputStream = codec.createOutputStream(fs.create(outFile))) {
      IOUtils.copyBytes(inputStream, outputStream, fs.getConf(), false);
    }
  }
}