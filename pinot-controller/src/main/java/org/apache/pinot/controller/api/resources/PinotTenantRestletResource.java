/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.EnumUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.SWAGGER_AUTHORIZATION_KEY;


/**
 * <ul>
 *   <li>Sample curl call to create broker tenant</li>
 *   curl -i -X POST -H 'Content-Type: application/json' -d
 *   '{
 *     "role" : "broker",
 *     "numberOfInstances : "5",
 *     "name" : "brokerOne"
 *   }' http://localhost:1234/tenants
 *
 *   <li>Sample curl call to create server tenant</li>
 *   curl -i -X POST -H 'Content-Type: application/json' -d
 *   '{
 *     "role" : "server",
 *     "numberOfInstances : "5",
 *     "name" : "serverOne",
 *     "offlineInstances" : "3",
 *     "realtimeInstances" : "2"
 *   }' http://localhost:1234/tenants
 * </ul>
 */
@Api(tags = Constants.TENANT_TAG, authorizations = {@Authorization(value = SWAGGER_AUTHORIZATION_KEY)})
@SwaggerDefinition(securityDefinition = @SecurityDefinition(apiKeyAuthDefinitions = @ApiKeyAuthDefinition(name =
    HttpHeaders.AUTHORIZATION, in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER, key = SWAGGER_AUTHORIZATION_KEY)))
@Path("/")
public class PinotTenantRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTenantRestletResource.class);
  private static final String TENANT_NAME = "tenantName";
  private static final String TABLES = "tables";

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerMetrics _controllerMetrics;

  @POST
  @Path("/tenants")
  @Authenticate(AccessType.CREATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = " Create a tenant")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error creating tenant")
  })
  public SuccessResponse createTenant(Tenant tenant) {
    PinotResourceManagerResponse response;
    switch (tenant.getTenantRole()) {
      case BROKER:
        response = _pinotHelixResourceManager.createBrokerTenant(tenant);
        break;
      case SERVER:
        response = _pinotHelixResourceManager.createServerTenant(tenant);
        break;
      default:
        throw new RuntimeException("Not a valid tenant creation call");
    }
    if (response.isSuccessful()) {
      return new SuccessResponse("Successfully created tenant");
    }
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_CREATE_ERROR, 1L);
    throw new ControllerApplicationException(LOGGER, "Failed to create tenant", Response.Status.INTERNAL_SERVER_ERROR);
  }

  /*
   * For tenant update
   */
  // TODO: should be /tenant/{tenantName}
  @PUT
  @Path("/tenants")
  @Authenticate(AccessType.UPDATE)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update a tenant")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Failed to update the tenant")
  })
  public SuccessResponse updateTenant(Tenant tenant) {
    PinotResourceManagerResponse response;
    switch (tenant.getTenantRole()) {
      case BROKER:
        response = _pinotHelixResourceManager.updateBrokerTenant(tenant);
        break;
      case SERVER:
        response = _pinotHelixResourceManager.updateServerTenant(tenant);
        break;
      default:
        throw new RuntimeException("Not a valid tenant update call");
    }
    if (response.isSuccessful()) {
      return new SuccessResponse("Updated tenant");
    }
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_UPDATE_ERROR, 1L);
    throw new ControllerApplicationException(LOGGER, "Failed to update tenant", Response.Status.INTERNAL_SERVER_ERROR);
  }

  public static class TenantMetadata {
    @JsonProperty(value = "ServerInstances")
    Set<String> _serverInstances;
    @JsonProperty(value = "OfflineServerInstances")
    Set<String> _offlineServerInstances;
    @JsonProperty(value = "RealtimeServerInstances")
    Set<String> _realtimeServerInstances;
    @JsonProperty(value = "BrokerInstances")
    Set<String> _brokerInstances;
    @JsonProperty(TENANT_NAME)
    String _tenantName;
  }

  @GET
  @Path("/tenants")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List all tenants")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error reading tenants list")
  })
  public TenantsList getAllTenants(
      @ApiParam(value = "Tenant type", required = false, allowableValues = "BROKER, SERVER", defaultValue = "")
      @QueryParam("type") @DefaultValue("") String type) {
    TenantsList tenants = new TenantsList();

    if (type == null || type.isEmpty() || type.equalsIgnoreCase("server")) {
      tenants._serverTenants = _pinotHelixResourceManager.getAllServerTenantNames();
    }
    if (type == null || type.isEmpty() || type.equalsIgnoreCase("broker")) {
      tenants._brokerTenants = _pinotHelixResourceManager.getAllBrokerTenantNames();
    }
    return tenants;
  }

  @GET
  @Path("/tenants/{tenantName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List instance for a tenant")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error reading tenants list")
  })
  public String listInstance(
      @ApiParam(value = "Tenant name", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "Tenant type (server|broker)") @QueryParam("type") String tenantType,
      @ApiParam(value = "Table type (offline|realtime)") @QueryParam("tableType") String tableType) {
    return listInstancesForTenant(tenantName, tenantType, tableType);
  }

  @POST
  @Path("/tenants/{tenantName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "enable/disable/drop a tenant")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error applying state to tenant")
  })
  public SuccessResponse toggleTenantState(
      @ApiParam(value = "Tenant name", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "Tenant type (server|broker)") @QueryParam("type") String tenantType,
      @ApiParam(value = "Table type (offline|realtime)") @QueryParam("tableType") String tableType,
      @ApiParam(value = "state") @QueryParam("state") String stateStr)
      throws Exception {
    if (stateStr == null || !EnumUtils.isValidEnumIgnoreCase(StateType.class, stateStr)) {
      throw new ControllerApplicationException(LOGGER,
          "Error: State mentioned " + stateStr + " is wrong. Valid States: Enable, Disable, Drop",
          Response.Status.BAD_REQUEST);
    } else {
      return toggleTenantState(tenantName, stateStr, tenantType);
    }
  }

  /**
   * This method expects a tenant name and will return a list of tables tagged on that tenant. It assumes that the
   * tagname is for server tenants only.
   * @param tenantName
   * @return
   */
  @GET
  @Path("/tenants/{tenantName}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List tables on a a server tenant")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Error reading list")
  })
  public String getTablesOnTenant(
      @ApiParam(value = "Tenant name", required = true) @PathParam("tenantName") String tenantName) {
    return getTablesServedFromTenant(tenantName);
  }

  private String getTablesServedFromTenant(String tenantName) {
    Set<String> tables = new HashSet<>();
    ObjectNode resourceGetRet = JsonUtils.newObjectNode();

    for (String table : _pinotHelixResourceManager.getAllTables()) {
      TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(table);
      if (tableConfig == null) {
        LOGGER.error("Unable to retrieve table config for table: {}", table);
        continue;
      }
      String tableConfigTenant = tableConfig.getTenantConfig().getServer();
      if (tenantName.equals(tableConfigTenant)) {
        tables.add(table);
      }
    }

    resourceGetRet.set(TABLES, JsonUtils.objectToJsonNode(tables));
    return resourceGetRet.toString();
  }

  private SuccessResponse toggleTenantState(String tenantName, String stateStr, @Nullable String tenantType) {
    Set<String> serverInstances = new HashSet<>();
    Set<String> brokerInstances = new HashSet<>();
    ObjectNode instanceResult = JsonUtils.newObjectNode();

    if ((tenantType == null) || tenantType.equalsIgnoreCase("server")) {
      serverInstances = _pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName);
    }

    if ((tenantType == null) || tenantType.equalsIgnoreCase("broker")) {
      brokerInstances = _pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName);
    }

    Set<String> allInstances = new HashSet<String>(serverInstances);
    allInstances.addAll(brokerInstances);

    if (StateType.DROP.name().equalsIgnoreCase(stateStr)) {
      if (!allInstances.isEmpty()) {
        throw new ControllerApplicationException(LOGGER,
            "Error: Tenant " + tenantName + " has live instances, cannot be dropped.", Response.Status.BAD_REQUEST);
      }
      _pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
      _pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
      _pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
      return new SuccessResponse("Dropped tenant " + tenantName + " successfully.");
    }

    if (StateType.ENABLE.name().equalsIgnoreCase(stateStr)) {
      for (String instance : allInstances) {
        instanceResult.put(instance, JsonUtils.objectToJsonNode(_pinotHelixResourceManager.enableInstance(instance)));
      }
      return new SuccessResponse("Enabled tenant " + tenantName + " successfully.");
    }

    if (StateType.DISABLE.name().equalsIgnoreCase(stateStr)) {
      for (String instance : allInstances) {
        instanceResult.put(instance, JsonUtils.objectToJsonNode(_pinotHelixResourceManager.disableInstance(instance)));
      }
      return new SuccessResponse("Disabled tenant " + tenantName + " successfully.");
    }

    return new SuccessResponse("No-Op done on tenant " + tenantName);
  }

  private String listInstancesForTenant(String tenantName, String tenantType, String tableTypeString) {
    ObjectNode resourceGetRet = JsonUtils.newObjectNode();

    List<InstanceConfig> instanceConfigList = _pinotHelixResourceManager.getAllHelixInstanceConfigs();

    if (tenantType == null) {
      Set<String> allServerInstances =
          _pinotHelixResourceManager.getAllInstancesForServerTenant(instanceConfigList, tenantName);
      Set<String> allBrokerInstances =
          _pinotHelixResourceManager.getAllInstancesForBrokerTenant(instanceConfigList, tenantName);

      if (allServerInstances.isEmpty() && allBrokerInstances.isEmpty()) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to find any instances for broker and server tenants: " + tenantName, Response.Status.NOT_FOUND);
      }
      resourceGetRet.set("ServerInstances", JsonUtils.objectToJsonNode(allServerInstances));
      resourceGetRet.set("BrokerInstances", JsonUtils.objectToJsonNode(allBrokerInstances));
    } else {
      if (tenantType.equalsIgnoreCase("server")) {
        Set<String> allServerInstances = new HashSet<>();
        TableType tableType = null;
        if (tableTypeString != null) {
          tableType = TableType.valueOf(tableTypeString.toUpperCase());
        }
        if (tableType == null || tableType == TableType.OFFLINE) {
          Set<String> offlineServerInstances = _pinotHelixResourceManager
              .getAllInstancesForServerTenantWithType(instanceConfigList, tenantName, TableType.OFFLINE);
          resourceGetRet.set("OfflineServerInstances", JsonUtils.objectToJsonNode(offlineServerInstances));
          allServerInstances.addAll(offlineServerInstances);
        }
        if (tableType == null || tableType == TableType.REALTIME) {
          Set<String> realtimeServerInstances = _pinotHelixResourceManager
              .getAllInstancesForServerTenantWithType(instanceConfigList, tenantName, TableType.REALTIME);
          resourceGetRet.set("RealtimeServerInstances", JsonUtils.objectToJsonNode(realtimeServerInstances));
          allServerInstances.addAll(realtimeServerInstances);
        }
        if (allServerInstances.isEmpty()) {
          throw new ControllerApplicationException(LOGGER,
              "Failed to find any instances for server tenant: " + tenantName + (tableType != null ? "_" + tableType
                  .name() : ""), Response.Status.NOT_FOUND);
        }
        resourceGetRet.set("ServerInstances", JsonUtils.objectToJsonNode(allServerInstances));
      }
      if (tenantType.equalsIgnoreCase("broker")) {
        Set<String> allBrokerInstances =
            _pinotHelixResourceManager.getAllInstancesForBrokerTenant(instanceConfigList, tenantName);

        if (allBrokerInstances.isEmpty()) {
          throw new ControllerApplicationException(LOGGER,
              "Failed to find any instances for broker tenant: " + tenantName, Response.Status.NOT_FOUND);
        }
        resourceGetRet.set("BrokerInstances", JsonUtils.objectToJsonNode(allBrokerInstances));
      }
    }
    resourceGetRet.put(TENANT_NAME, tenantName);
    return resourceGetRet.toString();
  }

  @GET
  @Path("/tenants/{tenantName}/metadata")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get tenant information")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = TenantMetadata.class),
      @ApiResponse(code = 404, message = "Tenant not found"),
      @ApiResponse(code = 500, message = "Server error reading tenant information")
  })
  public TenantMetadata getTenantMetadata(
      @ApiParam(value = "Tenant name", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "tenant type", required = false, defaultValue = "", allowableValues = "SERVER, BROKER")
      @QueryParam("type") @DefaultValue("") String type) {

    TenantMetadata tenantMeta = new TenantMetadata();
    if (type == null || type.isEmpty()) {
      tenantMeta._serverInstances = _pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName);
      tenantMeta._brokerInstances = _pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName);
    } else {
      if (type.equalsIgnoreCase("server")) {
        tenantMeta._serverInstances = _pinotHelixResourceManager.getAllInstancesForServerTenant(tenantName);
      }
      if (type.equalsIgnoreCase("broker")) {
        tenantMeta._brokerInstances = _pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenantName);
      }
    }
    tenantMeta._tenantName = tenantName;
    return tenantMeta;
  }

  public static class TenantsList {
    @JsonProperty("SERVER_TENANTS")
    Set<String> _serverTenants;
    @JsonProperty("BROKER_TENANTS")
    Set<String> _brokerTenants;
  }

  // GET ?? really ??
  // TODO: FIXME: This API is horribly bad design doing too many unrelated operations and giving
  // different responses for each. That's a bad way to structure APIs because clients have no good way
  // to parse response. Maintaining old behavior for backward compatibility.
  // CHANGE-ALERT: This is not backward compatible. We've changed this API from GET to POST because:
  //   1. That is correct
  //   2. with GET, we need to write our own routing logic to avoid conflict since this is same as the API above
  @POST
  @Path("/tenants/{tenantName}/metadata")
  @Authenticate(AccessType.UPDATE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Change tenant state")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = String.class),
      @ApiResponse(code = 404, message = "Tenant not found"),
      @ApiResponse(code = 500, message = "Server error reading tenant information")
  })
  public String changeTenantState(
      @ApiParam(value = "Tenant name", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "tenant type", required = false, defaultValue = "", allowableValues = "SERVER, BROKER")
      @QueryParam("type") String type,
      @ApiParam(value = "state", required = true, defaultValue = "", allowableValues = "enable, disable, drop")
      @QueryParam("state") @DefaultValue("") String state) {
    TenantMetadata tenantMetadata = getTenantMetadata(tenantName, type);
    Set<String> allInstances = new HashSet<>();
    if (tenantMetadata._brokerInstances != null) {
      allInstances.addAll(tenantMetadata._brokerInstances);
    }
    if (tenantMetadata._serverInstances != null) {
      allInstances.addAll(tenantMetadata._serverInstances);
    }
    // TODO: do not support drop. It's same as DELETE
    if (StateType.DROP.name().equalsIgnoreCase(state)) {
      if (!allInstances.isEmpty()) {
        throw new ControllerApplicationException(LOGGER, "Tenant " + tenantName + " has live instance",
            Response.Status.BAD_REQUEST);
      }
      _pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
      _pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
      _pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
      try {
        return JsonUtils.objectToString(new SuccessResponse("Deleted tenant " + tenantName));
      } catch (JsonProcessingException e) {
        LOGGER.error("Error serializing response to json");
        return "{\"message\" : \"Deleted tenant\" " + tenantName + "}";
      }
    }

    boolean enable = StateType.ENABLE.name().equalsIgnoreCase(state) ? true : false;
    ObjectNode instanceResult = JsonUtils.newObjectNode();
    String instance = null;
    try {
      for (String i : allInstances) {
        instance = i;
        if (enable) {
          instanceResult.set(instance, JsonUtils.objectToJsonNode(_pinotHelixResourceManager.enableInstance(instance)));
        } else {
          instanceResult
              .set(instance, JsonUtils.objectToJsonNode(_pinotHelixResourceManager.disableInstance(instance)));
        }
      }
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_INSTANCE_POST_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER,
          String.format("Error during %s operation for instance: %s", type, instance),
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    return instanceResult.toString();
  }

  @DELETE
  @Path("/tenants/{tenantName}")
  @Authenticate(AccessType.DELETE)
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Delete a tenant")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Tenant can not be deleted"),
      @ApiResponse(code = 404, message = "Tenant not found"),
      @ApiResponse(code = 500, message = "Error deleting tenant")
  })
  public SuccessResponse deleteTenant(
      @ApiParam(value = "Tenant name", required = true) @PathParam("tenantName") String tenantName,
      @ApiParam(value = "Tenant type", required = true, allowableValues = "SERVER, BROKER") @QueryParam("type")
      @DefaultValue("") String type) {

    if (type == null || type.isEmpty()) {
      throw new ControllerApplicationException(LOGGER, "Tenant type (BROKER | SERVER) is required as query parameter",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    TenantRole tenantRole = TenantRole.valueOf(type.toUpperCase());
    PinotResourceManagerResponse res = null;
    switch (tenantRole) {
      case BROKER:
        if (_pinotHelixResourceManager.isBrokerTenantDeletable(tenantName)) {
          res = _pinotHelixResourceManager.deleteBrokerTenantFor(tenantName);
        } else {
          throw new ControllerApplicationException(LOGGER, "Broker tenant is not null, can not delete it",
              Response.Status.BAD_REQUEST);
        }
        break;
      case SERVER:
        if (_pinotHelixResourceManager.isServerTenantDeletable(tenantName)) {
          res = _pinotHelixResourceManager.deleteOfflineServerTenantFor(tenantName);
          if (res.isSuccessful()) {
            res = _pinotHelixResourceManager.deleteRealtimeServerTenantFor(tenantName);
          }
        } else {
          throw new ControllerApplicationException(LOGGER, "Server tenant is not null, can not delete it",
              Response.Status.BAD_REQUEST);
        }
        break;
      default:
        break;
    }
    if (res.isSuccessful()) {
      return new SuccessResponse("Successfully deleted tenant " + tenantName);
    }
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_TENANT_DELETE_ERROR, 1L);
    throw new ControllerApplicationException(LOGGER, "Error deleting tenant", Response.Status.INTERNAL_SERVER_ERROR);
  }
}
