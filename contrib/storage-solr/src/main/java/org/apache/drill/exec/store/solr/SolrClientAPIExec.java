/**
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
package org.apache.drill.exec.store.solr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.exec.store.solr.schema.CVSchema;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.codehaus.jackson.map.ObjectMapper;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;

public class SolrClientAPIExec {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(SolrClientAPIExec.class);
  private SolrClient solrClient;

  public SolrClient getSolrClient() {
    return solrClient;
  }

  public void setSolrClient(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public SolrClientAPIExec(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  public SolrClientAPIExec() {

  }

  public Set<String> getSolrCoreList() {
    // Request core list
    logger.debug("getting cores from solr..");
    CoreAdminRequest request = new CoreAdminRequest();
    request.setAction(CoreAdminAction.STATUS);
    Set<String> coreList = null;
    try {
      CoreAdminResponse cores = request.process(solrClient);
      coreList = new HashSet<String>(cores.getCoreStatus().size());
      for (int i = 0; i < cores.getCoreStatus().size(); i++) {
        coreList.add(cores.getCoreStatus().getName(i));
      }
    } catch (SolrServerException | IOException e) {
      logger.error("error getting core info from solr server...");
    }
    return coreList;
  }

  public CVSchema getSchemaForCore(String coreName) {
    String schemaUrl = "http://dm2perf:20000/servlets/collection?type=GETSCHEMAFIELDS&corename={0}";
    schemaUrl = MessageFormat.format(schemaUrl, coreName);
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(schemaUrl);
    CVSchema oCVSchema = null;
    request.setHeader("Content-Type", "application/json");
    try {
      logger.info("getting schema details for core... " + coreName
          + " schemaUrl :" + schemaUrl);
      HttpResponse response = client.execute(request);
      BufferedReader rd = new BufferedReader(new InputStreamReader(response
          .getEntity().getContent()));
      StringBuffer result = new StringBuffer();
      String line = "";
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }
      logger.info("schema info... " + result);
      ObjectMapper mapper = new ObjectMapper();
      oCVSchema = mapper.readValue(result.toString(), CVSchema.class);
    } catch (Exception e) {
      logger.info("exception occured while fetching schema details..."
          + e.getMessage());
    }
    return oCVSchema;
  }

  public SolrDocumentList getSolrDocs(String solrServer, String solrCoreName,
      List<String> fields) {
    SolrClient solrClient = new HttpSolrClient(solrServer + solrCoreName);
    SolrDocumentList sList = null;
    SolrQuery solrQuery = new SolrQuery().setTermsRegexFlag("case_insensitive")
        .setRows(Integer.MAX_VALUE);
    solrQuery.setParam("q", "*:*");
    if (fields != null) {
      logger.info("solr fields are " + fields);
      String fieldStr = Joiner.on(",").join(fields);
      solrQuery.setParam("fl", fieldStr);
    }
    try {
      QueryResponse rsp = solrClient.query(solrQuery);
      sList = rsp.getResults();
    } catch (SolrServerException | IOException e) {
      logger.info("error occured while fetching results from solr server "
          + e.getMessage());
    }
    return sList;
  }

  public List<Tuple> getSolrResponse(String solrServer, SolrClient solrClient,
      String solrCoreName, Map<String, String> solrParams) {
    logger.info("sending request to solr server " + solrServer + " on core "
        + solrCoreName);
    SolrStream solrStream = new SolrStream(solrServer, solrParams);
    List<Tuple> resultTuple = Lists.newArrayList();
    try {
      solrStream.open();

      Tuple tuple = null;
      while (true) {
        tuple = solrStream.read();
        if (tuple.EOF) {
          break;
        }
        resultTuple.add(tuple);
      }
    } catch (Exception e) {
      logger.info("error occured while fetching results from solr server "
          + e.getMessage());
    } finally {
      try {
        solrStream.close();
      } catch (IOException e) {
        logger.info("error occured while fetching results from solr server "
            + e.getMessage());
      }

    }
    return resultTuple;
  }

}
