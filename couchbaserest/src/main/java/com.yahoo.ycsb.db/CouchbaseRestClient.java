/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.google.gson.stream.JsonReader;
import com.yahoo.ycsb.*;
import org.apache.http.*;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import java.io.*;
import java.util.*;
import org.apache.http.util.EntityUtils;

/**
 * A couchbase workload runner using the n1ql rest api
 * couchbase.bucket Couchbase bucket to connect
 * couchbase.maxParallelism  Number of the cpu cores that can be used by the N1QL Server
 * couchbase.n1qlhosts Comma separated N1QL hosts
 * couchbase.queryEndpoints Max number of connection in pool
 * couchbase.adhoc Use prepared queries support
 *
 * @author Subhashni Balakrishan
 */
public class CouchbaseRestClient extends DB {
  private static final Object INIT_COORDINATOR = new Object();

  public static final String BUCKET_PROPERTY = "couchbase.bucket";
  public static final String ADHOC_PROPOERTY = "couchbase.adhoc";
  public static final String MAX_PARALLEL_PROPERTY = "couchbase.maxParallelism";
  public static final String N1QLHOSTS_PROPERTY = "couchbase.n1qlhosts";
  public static final String N1QL_QUERYENDPOINTS = "couchbase.queryEndpoints";

  public static final int N1QL_PORT = 8093;
  public static final int QUERY_TIMEOUT = 75000;

  private String bucketName;
  private boolean adhoc;
  private int maxParallelism;
  private String[] n1qlhosts;
  private Map<String, Map<String, String>> prepareCache;
  private Gson gson;
  private JsonParser jsonParser;
  private CloseableHttpClient ht = null;
  private PoolingHttpClientConnectionManager connectionManager = null;
  private int maxQueryEndPoints;
  private HttpPost httpPost;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance;
   */

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    bucketName = props.getProperty(BUCKET_PROPERTY, "default");
    adhoc = props.getProperty(ADHOC_PROPOERTY, "false").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty(MAX_PARALLEL_PROPERTY, "1"));
    n1qlhosts = props.getProperty(N1QLHOSTS_PROPERTY, "127.0.0.1").split(",");
    prepareCache = new HashMap<String, Map<String, String>>();
    maxQueryEndPoints = Integer.parseInt(props.getProperty(N1QL_QUERYENDPOINTS, "100"));
    gson = new Gson();
    jsonParser = new JsonParser();

    ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
      @Override
      public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
        // Honor 'keep-alive' header
        HeaderElementIterator it = new BasicHeaderElementIterator(
          response.headerIterator(HTTP.CONN_KEEP_ALIVE));
        while (it.hasNext()) {
          HeaderElement he = it.nextElement();
          String param = he.getName();
          String value = he.getValue();
          if (value != null && param.equalsIgnoreCase("timeout")) {
            try {
              return Long.parseLong(value) * 1000;
            } catch(NumberFormatException ignore) {
            }
          }
        }
        return 1000 * 1000 * 1000;
      }

    };
    //use a pooling connection manager so connections can be reused
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(maxQueryEndPoints);
    SocketConfig socketConfig = SocketConfig.custom()
      .setSoKeepAlive(true)
      .setTcpNoDelay(true)
      .build();

    ht = HttpClients.custom()
      .setConnectionManager(connectionManager)
      .setKeepAliveStrategy(keepAliveStrategy)
      .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
      .setDefaultSocketConfig(socketConfig)
      .build();

  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance;
   */
  @Override
  public void cleanup() throws DBException {
    try {
      connectionManager.shutdown();
    } catch (Exception ex) {
      System.out.println(ex.getMessage());
    }
  }

  @Override
  public Status read(final String table, final String key, Set<String> fields,
                     final HashMap<String, ByteIterator> result) {

    String readQuery = "SELECT " + joinSet(bucketName, fields) + " FROM `"
            + bucketName + "` USE KEYS [$1]";
    JsonArray args = new JsonArray();
    args.add(formatId(table, key));
    return query(readQuery, args);
  }

  @Override
  public Status update(final String table, final String key,
                       final HashMap<String, ByteIterator> values) {
    String fields = encodeN1qlFields(values);
    String updateQuery = "UPDATE `" + bucketName + "` USE KEYS [$1] SET " + fields;
    JsonArray args = new JsonArray();
    args.add(formatId(table, key));
    return query(updateQuery, args);
  }


  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    String insertQuery = "UPSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";
    JsonArray args = new JsonArray();
    args.add(formatId(table, key));
    args.add(encodeIntoJson(values));
    return query(insertQuery, args);
  }

  @Override
  public Status delete(final String table, final String key) {
    String deleteQuery = "DELETE FROM `" + bucketName + "` USE KEYS [$1]";
    JsonArray args = new JsonArray();
    args.add(formatId(table, key));
    return query(deleteQuery, args);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    if (fields == null || fields.isEmpty()) {
        fields = new HashSet<String>(Arrays.asList("field0", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9"));
    }
    String scanQuery = "SELECT " + joinSet(bucketName, fields) + " FROM `"
            + bucketName + "` WHERE meta().id >= '$1' LIMIT $2";
    JsonArray args = new JsonArray();
    args.add(formatId(table, startkey));
    args.add(recordcount);
    Status s = query(scanQuery, args);
    return s;

  }

  private static String joinSet(final String bucket, final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (String f : fields) {
      builder.append("`").append(f).append("`").append(",");
    }
    String toReturn = builder.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  private static String formatId(final String prefix, final String key) {
    return prefix + ":" + key;
  }

  private String getN1QLHost() {
    int randomSelect = new Random().nextInt(n1qlhosts.length);
    return this.n1qlhosts[randomSelect < 0 ? 0 : randomSelect];
  }

  private static JsonObject encodeIntoJson(final HashMap<String, ByteIterator> values) {
    JsonObject result = new JsonObject();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      result.addProperty(entry.getKey(), entry.getValue().toString());
    }
    return result;
  }

  private static String encodeN1qlFields(final HashMap<String, ByteIterator> values) {
    if (values.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String raw = entry.getValue().toString();
      String escaped = raw.replace("\"", "\\\"").replace("\'", "\\\'");
      sb.append(entry.getKey()).append("=\"").append(escaped).append("\" ");

    }
    String toReturn = sb.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  private Status query(String query, JsonArray args) {
    try {
      List<NameValuePair> nvps = new ArrayList<>();
      String jsonData = "";
      String host = getN1QLHost();

      if (!adhoc) {
        if (!prepareCache.containsKey(query+host)) {
          if (!executePrepare(query, host)) {
            return Status.ERROR;
          }
        }
        Map<String, String> prepared = prepareCache.get(query+host);
        JsonObject obj = new JsonObject();
        obj.addProperty("prepared", prepared.get("name").toString());
        //Do not include encoded_plan as it adds additional overhead to request
        //obj.addProperty("encoded_plan", prepared.get("encoded_plan").toString());
        obj.add("args", args);
        jsonData = obj.toString();
      } else {
        nvps.add(new BasicNameValuePair("statement", query));
        nvps.add(new BasicNameValuePair("args", args.toString()));
      }

      nvps.add(new BasicNameValuePair("timeout", Integer.toString(QUERY_TIMEOUT) + "s"));
      nvps.add(new BasicNameValuePair("max_parallelism", Integer.toString(maxParallelism)));
      JsonObject obj = executeRequest(nvps, host, jsonData);

      if (obj != null) {
        if (obj.has("errors")) {
          System.out.println("Errors:" + obj.get("errors").toString());
          return Status.ERROR;
        }
      } else {
        System.out.println("No content found");
        return Status.ERROR;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;

  }

  private boolean executePrepare(String query, String host) {
    try {
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair("statement", "PREPARE " + query));
      JsonObject obj = executeRequest(nvps, host, "");

      if (obj != null && obj.has("results")) {
        JsonArray arr = obj.getAsJsonArray("results");
        JsonObject results = (JsonObject) arr.get(0);

        Map<String, String> prepared = new HashMap<>();
        prepared.put("name", results.get("name").getAsString());
        prepared.put("encoded_plan", results.get("encoded_plan").getAsString());
        prepareCache.put(query+host, prepared);
      } else {
        throw new DBException("Prepare request got a bad response" + obj.toString());
      }
    } catch (DBException ex) {
      ex.printStackTrace();
      return false;
    }
    return true;
  }

  private JsonObject executeRequest(List<NameValuePair> nvps, String host, String jsonData) throws DBException {
    httpPost = new HttpPost("http://" + host + ":" + Integer.toString(N1QL_PORT) + "/query/service");
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Accept-Encoding", "*/*");
    try {

      if (jsonData != "") {
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setEntity(new StringEntity(jsonData));
      } else {
        httpPost.setHeader("Content-type", "application/x-www-form-urlencoded; charset=UTF-8");
        httpPost.setEntity(new UrlEncodedFormEntity(nvps));
      }
    } catch (UnsupportedEncodingException ex) {
      throw new DBException("Unsupported encoding exception" + ex.getMessage());
    }
    try {
      CloseableHttpResponse response = ht.execute(httpPost);

      HttpEntity entity = response.getEntity();
      int statusCode = response.getStatusLine().getStatusCode();
      JsonReader jsonReader = new JsonReader(new InputStreamReader(entity.getContent()));
      Object obj = jsonParser.parse(jsonReader);
      if (statusCode != 200) {
        String msg = "Query rest call returned a not OK status: " + statusCode;
        if (obj != null) {
          System.out.println(obj.toString());
        }
        throw new DBException(msg + response.getStatusLine());
      }
      EntityUtils.consume(entity);
      response.close();
      return (JsonObject) obj;
    } catch (Exception ex) {
      throw new DBException(ex.getMessage() + ex.getStackTrace());
    } finally {
      httpPost.releaseConnection();
    }
  }
}
