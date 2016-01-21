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
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.http.message.BasicNameValuePair;

import java.io.*;
import java.util.*;

/**
 * A class that wraps the 1.x CouchbaseClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 * <p/>
 * <p> The following options must be passed when using this database client.
 * <p/>
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=default</b> The bucket name to use./li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.syncMutationResponse=true</b> If mutations should wait for the response to complete.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.adhoc=false</b> If set to true, prepared statements are not used.</li>
 * <li><b>couchbase.kv=true</b> If set to false, mutation operations will also be performed through N1QL.</li>
 * <li><b>couchbase.maxParallelism=1</b> The server parallelism for all n1ql queries.</li>
 * </ul>
 *
 * @author Subhashni Balakrishan
 */
public class CouchbaseRestClient extends DB {
  public static final String BUCKET_PROPERTY = "couchbase.bucket";
  public static final String ADHOC_PROPOERTY = "couchbase.adhoc";
  public static final String MAX_PARALLEL_PROPERTY = "couchbase.maxParallelism";
  public static final String N1QLHOSTS_PROPERTY = "couchbase.n1qlhosts";
  public static final String UPSERT_PROPERTY = "couchbase.upsert";

  public static final int N1QL_PORT = 8093;
  public static final int QUERY_TIMEOUT = 75000;

  private String bucketName;

  private boolean adhoc;
  private int maxParallelism;
  private String[] n1qlhosts;
  private Map<String, Map<String, String>> prepareCache;
  private Gson gson;
  private JsonParser jsonParser;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    bucketName = props.getProperty(BUCKET_PROPERTY, "default");
    adhoc = props.getProperty(ADHOC_PROPOERTY, "false").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty(MAX_PARALLEL_PROPERTY, "1"));
    n1qlhosts = props.getProperty(N1QLHOSTS_PROPERTY, "127.0.0.1").split(",");
    prepareCache = new HashMap<String, Map<String, String>>();
    gson = new Gson();
    jsonParser = new JsonParser();
  }

  @Override
  public void cleanup() throws DBException {
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
    String insertQuery = "INSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";
    JsonArray args = new JsonArray();
    args.add(formatId(table, key));
    args.add(encodeIntoJson(values));
    return query(insertQuery, args);
  }

  private Status upsert(String table, String key, HashMap<String, ByteIterator> values) {
    String upsertQuery = "UPSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";
    JsonArray args = new JsonArray();
    args.add(formatId(table, key));
    args.add(encodeIntoJson(values));
    return query(upsertQuery, args);
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
      return "`" + bucket + "`.*";
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
    int randomSelect = new Random().nextInt(n1qlhosts.length) - 1;
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

      if (adhoc) {
        if (!prepareCache.containsKey(query)) {
          if (!executePrepare(query)) {
            return Status.ERROR;
          }
        }
        Map<String, String> prepared = prepareCache.get(query);
        JsonObject obj = new JsonObject();
        obj.addProperty("prepared", prepared.get("name").toString());
        obj.addProperty("encoded_plan", prepared.get("encoded_plan").toString());
        obj.add("args", args);
        jsonData = obj.toString();
      } else {
        nvps.add(new BasicNameValuePair("statement", query));
        nvps.add(new BasicNameValuePair("args", args.toString()));
      }

      nvps.add(new BasicNameValuePair("timeout", Integer.toString(QUERY_TIMEOUT) + "s"));
      nvps.add(new BasicNameValuePair("max_parallelism", Integer.toString(maxParallelism)));
      JsonObject obj = executeRequest(nvps, jsonData);

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

  private boolean executePrepare(String query) {
    try {
      List<NameValuePair> nvps = new ArrayList<>();
      nvps.add(new BasicNameValuePair("statement", "PREPARE " + query));
      JsonObject obj = executeRequest(nvps, "");

      if (obj != null && obj.has("results")) {
        JsonArray arr = obj.getAsJsonArray("results");
        JsonObject results = (JsonObject) arr.get(0);

        Map<String, String> prepared = new HashMap<>();
        prepared.put("name", results.get("name").getAsString());
        prepared.put("encoded_plan", results.get("encoded_plan").getAsString());
        prepareCache.put(query, prepared);
      } else {
        throw new DBException("Prepare request got a bad response" + obj.toString());
      }
    } catch (DBException ex) {
      System.out.print("Prepare execution failed \n" + ex.getMessage() + "\n Stack trace\t" + ex.getStackTrace());
      return false;
    }
    return true;
  }

  private JsonObject executeRequest(List<NameValuePair> nvps, String jsonData) throws DBException {
    HttpPost httpPost = new HttpPost("http://" + getN1QLHost() + ":" + Integer.toString(N1QL_PORT) + "/query/service");

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
      CloseableHttpClient ht = HttpClients.createDefault();
      CloseableHttpResponse response = ht.execute(httpPost);
      HttpEntity entity = response.getEntity();
      int statusCode = response.getStatusLine().getStatusCode();
      JsonReader jsonReader = new JsonReader(new InputStreamReader(entity.getContent()));
      Object obj = jsonParser.parse(jsonReader);
      if (statusCode != 200) {
        System.out.println(obj.toString());
        String msg = "Query rest call returned a not OK status: " + statusCode;
        throw new DBException(msg + ":" + response.getStatusLine());
      }
      ht.close();
      return (JsonObject) obj;
    } catch (Exception ex) {
      throw new DBException(ex.getMessage() + ex.getStackTrace());
    }
  }
}
