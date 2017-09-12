/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.monitor.view;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.monitor.Monitor;
import org.glassfish.jersey.server.mvc.Template;

/**
 *
 * Index is responsible of specifying Monitor paths and setting the templates for the HTML code
 *
 * @since 2.0.0
 *
 */
@Path("/")
@Produces(MediaType.TEXT_HTML)
public class WebViews {

  private Map<String,Object> getModel() {

    Map<String,Object> model = new HashMap<>();
    model.put("version", Constants.VERSION);
    model.put("instance_name", Monitor.cachedInstanceName.get());
    model.put("instance_id", Monitor.getContext().getInstance().getInstanceID());
    return model;
  }

  /**
   * Returns the overview template
   *
   * @return Overview model
   */
  @GET
  @Template(name = "/default.ftl")
  public Map<String,Object> get() {

    Map<String,Object> model = getModel();
    model.put("title", "Accumulo Overview");
    model.put("template", "overview.ftl");
    model.put("js", "overview.js");

    return model;
  }

  /**
   * Returns the master template
   *
   * @return Master model
   */
  @GET
  @Path("{parameter: master|monitor}")
  @Template(name = "/default.ftl")
  public Map<String,Object> getMaster() {

    List<String> masters = Monitor.getContext().getInstance().getMasterLocations();

    Map<String,Object> model = getModel();
    model.put("title", "Master Server" + (masters.size() == 0 ? "" : ":" + AddressUtil.parseAddress(masters.get(0), false).getHost()));
    model.put("template", "master.ftl");
    model.put("js", "master.js");

    model.put("tablesTitle", "Table Status");
    model.put("tablesTemplate", "tables.ftl");
    model.put("tablesJs", "tables.js");
    return model;
  }

  /**
   * Returns the tservers templates
   *
   * @param server
   *          TServer to show details
   * @return tserver model
   */
  @GET
  @Path("tservers")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTabletServers(@QueryParam("s") String server) {

    Map<String,Object> model = getModel();
    model.put("title", "Tablet Server Status");
    if (server != null) {
      model.put("template", "server.ftl");
      model.put("js", "server.js");
      model.put("server", server);
      return model;
    }
    model.put("template", "tservers.ftl");
    model.put("js", "tservers.js");
    return model;
  }

  /**
   * Returns the scans template
   *
   * @return Scans model
   */
  @GET
  @Path("scans")
  @Template(name = "/default.ftl")
  public Map<String,Object> getScans() {

    Map<String,Object> model = getModel();
    model.put("title", "Scans");
    model.put("template", "scans.ftl");
    model.put("js", "scans.js");

    return model;
  }

  /**
   * Returns the bulk import template
   *
   * @return Bulk Import model
   */
  @GET
  @Path("bulkImports")
  @Template(name = "/default.ftl")
  public Map<String,Object> getBulkImports() {

    Map<String,Object> model = getModel();
    model.put("title", "Bulk Imports");
    model.put("template", "bulkImport.ftl");
    model.put("js", "bulkImport.js");

    return model;
  }

  /**
   * Returns the garbage collector template
   *
   * @return GC model
   */
  @GET
  @Path("gc")
  @Template(name = "/default.ftl")
  public Map<String,Object> getGC() {

    Map<String,Object> model = getModel();
    model.put("title", "Garbage Collector Status");
    model.put("template", "gc.ftl");
    model.put("js", "gc.js");

    return model;
  }

  /**
   * Returns the server activity template
   *
   * @param shape
   *          Shape of visualization
   * @param size
   *          Size of visualization
   * @param motion
   *          Motion of visualization
   * @param color
   *          Color of visualization
   * @return Server activity model
   */
  @GET
  @Path("vis")
  @Template(name = "/default.ftl")
  public Map<String,Object> getServerActivity(@QueryParam("shape") @DefaultValue("circles") String shape, @QueryParam("size") @DefaultValue("40") String size,
      @QueryParam("motion") @DefaultValue("") String motion, @QueryParam("color") @DefaultValue("allavg") String color) {

    Map<String,Object> model = getModel();
    model.put("title", "Server Activity");
    model.put("template", "vis.ftl");

    model.put("shape", shape);
    model.put("size", size);
    model.put("motion", motion);
    model.put("color", color);

    return model;
  }

  /**
   * Returns the tables template
   *
   * @return Tables model
   */
  @GET
  @Path("tables")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTables() throws TableNotFoundException {

    Map<String,Object> model = getModel();
    model.put("title", "Table Status"); // Need this for the browser tab title
    model.put("tablesTitle", "Table Status");
    model.put("template", "tables.ftl");
    model.put("js", "tables.js");

    return model;
  }

  /**
   * Returns participating tservers template
   *
   * @param tableID
   *          Table ID for participating tservers
   * @return Participating tservers model
   */
  @GET
  @Path("tables/{tableID}")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTables(@PathParam("tableID") String tableID) throws TableNotFoundException {

    String tableName = Tables.getTableName(Monitor.getContext().getInstance(), Table.ID.of(tableID));

    Map<String,Object> model = getModel();
    model.put("title", "Table Status");

    model.put("template", "table.ftl");
    model.put("js", "table.js");
    model.put("tableID", tableID);
    model.put("table", tableName);

    return model;
  }

  /**
   * Returns trace summary template
   *
   * @param minutes
   *          Range of minutes
   * @return Trace summary model
   */
  @GET
  @Path("trace/summary")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTracesSummary(@QueryParam("minutes") @DefaultValue("10") String minutes) {

    Map<String,Object> model = getModel();
    model.put("title", "Traces for the last&nbsp;" + minutes + "&nbsp;minute(s)");

    model.put("template", "summary.ftl");
    model.put("js", "summary.js");
    model.put("minutes", minutes);

    return model;
  }

  /**
   * Returns traces by type template
   *
   * @param type
   *          Type of trace
   * @param minutes
   *          Range of minutes
   * @return Traces by type model
   */
  @GET
  @Path("trace/listType")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTracesForType(@QueryParam("type") String type, @QueryParam("minutes") @DefaultValue("10") String minutes) {

    Map<String,Object> model = getModel();
    model.put("title", "Traces for " + type + " for the last " + minutes + " minute(s)");

    model.put("template", "listType.ftl");
    model.put("js", "listType.js");
    model.put("type", type);
    model.put("minutes", minutes);

    return model;
  }

  /**
   * Returns traces by ID template
   *
   * @param id
   *          ID of the traces
   * @return Traces by ID model
   */
  @GET
  @Path("trace/show")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTraceShow(@QueryParam("id") String id) {

    Map<String,Object> model = getModel();
    model.put("title", "Trace ID " + id);

    model.put("template", "show.ftl");
    model.put("js", "show.js");
    model.put("id", id);

    return model;
  }

  /**
   * Returns log report template
   *
   * @return Log report model
   */
  @GET
  @Path("log")
  @Template(name = "/default.ftl")
  public Map<String,Object> getLogs() {

    Map<String,Object> model = getModel();
    model.put("title", "Recent Logs");

    model.put("template", "log.ftl");
    model.put("js", "log.js");

    return model;
  }

  /**
   * Returns problem report template
   *
   * @param table
   *          Table ID to display problem details
   * @return Problem report model
   */
  @GET
  @Path("problems")
  @Template(name = "/default.ftl")
  public Map<String,Object> getProblems(@QueryParam("table") String table) {

    Map<String,Object> model = getModel();
    model.put("title", "Per-Table Problem Report");

    model.put("template", "problems.ftl");
    model.put("js", "problems.js");

    if (table != null) {
      model.put("table", table);
    }

    return model;
  }

  /**
   * Returns replication table template
   *
   * @return Replication model
   */
  @GET
  @Path("replication")
  @Template(name = "/default.ftl")
  public Map<String,Object> getReplication() {

    Map<String,Object> model = getModel();
    model.put("title", "Replication Overview");

    model.put("template", "replication.ftl");
    model.put("js", "replication.js");

    return model;
  }
}
