/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.monitor.view;

import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX_BLANK_OK;
import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX_TABLE_ID;
import static org.apache.accumulo.monitor.util.ParameterValidator.HOSTNAME_PORT_REGEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.monitor.Monitor;
import org.glassfish.jersey.server.mvc.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is responsible for specifying Monitor REST Endpoints and setting the templates for the
 * HTML code
 *
 * @since 2.0.0
 */
@Path("/")
@Produces(MediaType.TEXT_HTML)
public class WebViews {

  private static final Logger log = LoggerFactory.getLogger(WebViews.class);

  @Inject
  private Monitor monitor;

  /**
   * Get HTML for external CSS and JS resources from configuration. See ACCUMULO-4739
   *
   * @param model map of the MVC model
   */
  private void addExternalResources(Map<String,Object> model) {
    AccumuloConfiguration conf = monitor.getContext().getConfiguration();
    String resourcesProperty = conf.get(Property.MONITOR_RESOURCES_EXTERNAL);
    if (resourcesProperty.isBlank()) {
      return;
    }
    List<String> monitorResources = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      Collections.addAll(monitorResources,
          objectMapper.readValue(resourcesProperty, String[].class));
    } catch (IOException e) {
      log.error("Error Monitor Resources config property {}: {}",
          Property.MONITOR_RESOURCES_EXTERNAL, e);
      return;
    }
    if (!monitorResources.isEmpty()) {
      model.put("externalResources", monitorResources);
    }
  }

  private Map<String,Object> getModel() {

    Map<String,Object> model = new HashMap<>();
    model.put("version", Constants.VERSION);
    model.put("instance_name", monitor.getContext().getInstanceName());
    model.put("instance_id", monitor.getContext().getInstanceID());
    model.put("zk_hosts", monitor.getContext().getZooKeepers());
    addExternalResources(model);
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
   * Returns the manager template
   *
   * @return Manager model
   */
  @GET
  @Path("{parameter: manager|monitor}")
  @Template(name = "/default.ftl")
  public Map<String,Object> getManager() {

    Map<String,Object> model = getModel();
    model.put("title", "Manager Server");
    model.put("template", "manager.ftl");
    model.put("js", "manager.js");

    model.put("tablesTitle", "Table Status");
    model.put("tablesTemplate", "tables.ftl");
    return model;
  }

  /**
   * Returns the tservers templates
   *
   * @param server TServer to show details
   * @return tserver model
   */
  @GET
  @Path("tservers")
  @Template(name = "/default.ftl")
  public Map<String,Object>
      getTabletServers(@QueryParam("s") @Pattern(regexp = HOSTNAME_PORT_REGEX) String server) {

    Map<String,Object> model = getModel();
    model.put("title", "Tablet Server Status");
    if (server != null && !server.isBlank()) {
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
    model.put("title", "Active scans");
    model.put("template", "scans.ftl");
    model.put("js", "scans.js");

    return model;
  }

  /**
   * Returns the compactions template
   *
   * @return Scans model
   */
  @GET
  @Path("compactions")
  @Template(name = "/default.ftl")
  public Map<String,Object> getCompactions() {

    Map<String,Object> model = getModel();
    model.put("title", "Active Compactions");
    model.put("template", "compactions.ftl");
    model.put("js", "compactions.js");

    return model;
  }

  /**
   * Returns the compactions template
   *
   * @return Scans model
   */
  @GET
  @Path("ec")
  @Template(name = "/default.ftl")
  public Map<String,Object> getExternalCompactions() {
    var ccHost = monitor.getCoordinatorHost();

    Map<String,Object> model = getModel();
    model.put("title", "External Compactions");
    model.put("template", "ec.ftl");

    if (ccHost.isPresent()) {
      model.put("coordinatorRunning", true);
      model.put("js", "ec.js");
    } else {
      model.put("coordinatorRunning", false);
    }

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
   * Returns the tables template
   *
   * @return Tables model
   */
  @GET
  @Path("tables")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTables() {

    Map<String,Object> model = getModel();
    model.put("title", "Table Status"); // Need this for the browser tab title
    model.put("tablesTitle", "Table Status");
    model.put("template", "tables.ftl");

    return model;
  }

  /**
   * Returns participating tservers template
   *
   * @param tableID Table ID for participating tservers
   * @return Participating tservers model
   */
  @GET
  @Path("tables/{tableID}")
  @Template(name = "/default.ftl")
  public Map<String,Object> getTables(
      @PathParam("tableID") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX_TABLE_ID) String tableID)
      throws TableNotFoundException {

    String tableName = monitor.getContext().getTableName(TableId.of(tableID));

    Map<String,Object> model = getModel();
    model.put("title", "Table Status");

    model.put("template", "table.ftl");
    model.put("js", "table.js");
    model.put("tableID", tableID);
    model.put("table", tableName);

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

    return model;
  }

  /**
   * Returns problem report template
   *
   * @param table Table ID to display problem details
   * @return Problem report model
   */
  @GET
  @Path("problems")
  @Template(name = "/default.ftl")
  public Map<String,Object>
      getProblems(@QueryParam("table") @Pattern(regexp = ALPHA_NUM_REGEX_BLANK_OK) String table) {

    Map<String,Object> model = getModel();
    model.put("title", "Per-Table Problem Report");

    model.put("template", "problems.ftl");
    model.put("js", "problems.js");

    if (table != null && !table.isBlank()) {
      model.put("table", table);
    }

    return model;
  }

}
