/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.AdapterTypeNotFoundException;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.HttpExceptionHandler;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.ScheduleAlreadyExistsException;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.AppLifecycleService;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.adapter.AdapterAlreadyExistsException;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.adapter.AdapterStatus;
import co.cask.cdap.internal.app.runtime.adapter.AdapterTypeInfo;
import co.cask.cdap.internal.app.runtime.adapter.InvalidAdapterOperationException;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Sink;
import co.cask.cdap.proto.Source;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for managing application lifecycle.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class AppLifecycleHttpHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppLifecycleHttpHandler.class);

  /**
   * Configuration object passed from higher up.
   */
  private final CConfiguration configuration;

  private final ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory;

  /**
   * Factory for handling the location - can do both in either Distributed or Local mode.
   */
  private final LocationFactory locationFactory;

  private final Scheduler scheduler;

  /**
   * Runtime program service for running and managing programs.
   */
  private final ProgramRuntimeService runtimeService;

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;

  private final DiscoveryServiceClient discoveryServiceClient;
  private final PreferencesStore preferencesStore;
  private final AdapterService adapterService;
  private final NamespaceAdmin namespaceAdmin;
  private final AppLifecycleService appLifecycleService;
  private final HttpExceptionHandler exceptionHandler;

  @Inject
  public AppLifecycleHttpHandler(Authenticator authenticator, CConfiguration configuration,
                                 ManagerFactory<DeploymentInfo, ApplicationWithPrograms> managerFactory,
                                 LocationFactory locationFactory, Scheduler scheduler,
                                 ProgramRuntimeService runtimeService, StoreFactory storeFactory,
                                 DiscoveryServiceClient discoveryServiceClient, PreferencesStore preferencesStore,
                                 AdapterService adapterService, AppLifecycleService appLifecycleService,
                                 NamespaceAdmin namespaceAdmin, HttpExceptionHandler exceptionHandler) {
    super(authenticator);
    this.configuration = configuration;
    this.managerFactory = managerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.locationFactory = locationFactory;
    this.scheduler = scheduler;
    this.runtimeService = runtimeService;
    this.store = storeFactory.create();
    this.discoveryServiceClient = discoveryServiceClient;
    this.preferencesStore = preferencesStore;
    this.adapterService = adapterService;
    this.appLifecycleService = appLifecycleService;
    this.exceptionHandler = exceptionHandler;
  }

  /**
   * Deploys an application with the specified name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @PathParam("app-id") final String appId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName)
    throws IOException, NamespaceNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    return deployApplication(request, responder, namespace, appId, archiveName);
  }

  /**
   * Deploys an application.
   */
  @POST
  @Path("/apps")
  public BodyConsumer deploy(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") final String namespaceId,
                             @HeaderParam(ARCHIVE_NAME_HEADER) final String archiveName)
    throws IOException, NamespaceNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    return deployApplication(request, responder, namespace, null, archiveName);
  }

  /**
   * Gets the {@link ApplicationRecord}s describing the applications in the specified namespace.
   */
  @GET
  @Path("/apps")
  public void listApps(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId) throws NamespaceNotFoundException {

    Id.Namespace namespace = Id.Namespace.from(namespaceId);
    Collection<ApplicationSpecification> appSpecs = appLifecycleService.listApps(namespace);
    responder.sendJson(HttpResponseStatus.OK, makeAppRecords(appSpecs));
  }

  /**
   * Gets the {@link ApplicationRecord} describing an application.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getApp(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("app-id") final String appId) throws ApplicationNotFoundException {
    Id.Application app = Id.Application.from(namespaceId, appId);
    ApplicationSpecification appSpec = appLifecycleService.getApp(app);
    responder.sendJson(HttpResponseStatus.OK, makeAppRecord(appSpec));
  }

  /**
   * Deletes an application.
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") final String appId) throws Exception {

    Id.Application id = Id.Application.from(namespaceId, appId);

    // Deletion of a particular application is not allowed if that application is used by an adapter
    if (adapterService.getAdapterTypeInfo(Id.AdapterType.from(id)) != null) {
      responder.sendString(HttpResponseStatus.CONFLICT,
                           String.format("Cannot delete Application '%s' because it's an Adapter Type", appId));
      return;
    }

    AppFabricServiceStatus appStatus = appLifecycleService.deleteApp(id);
    responder.sendString(appStatus.getCode(), appStatus.getMessage());
  }

  /**
   * Deletes all applications in the specified namespace.
   */
  @DELETE
  @Path("/apps")
  public void deleteApps(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) throws Exception {
    Id.Namespace id = Id.Namespace.from(namespaceId);
    AppFabricServiceStatus status = appLifecycleService.deleteApps(id);
    responder.sendString(status.getCode(), status.getMessage());
  }

  /**
   * Retrieves all adapters in a given namespace.
   */
  @GET
  @Path("/adapters")
  public void listAdapters(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws NamespaceNotFoundException {
    responder.sendJson(HttpResponseStatus.OK, adapterService.getAdapters(Id.Namespace.from(namespaceId)));
  }

  /**
   * Retrieves an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}")
  public void getAdapter(HttpRequest request, HttpResponder responder,
                         @PathParam("namespace-id") String namespaceId,
                         @PathParam("adapter-id") String adapterId) throws AdapterNotFoundException {
    Id.Adapter adapter = Id.Adapter.from(namespaceId, adapterId);
    AdapterSpecification adapterSpec = adapterService.getAdapter(adapter);
    responder.sendJson(HttpResponseStatus.OK, adapterSpec);
  }

  /**
   * Starts/stops an adapter
   */
  @POST
  @Path("/adapters/{adapter-id}/{action}")
  public void startStopAdapter(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId,
                               @PathParam("action") String action)
    throws BadRequestException, NotFoundException, InvalidAdapterOperationException {

    try {
      Id.Adapter adapter = Id.Adapter.from(namespaceId, adapterId);
      if ("start".equals(action)) {
        adapterService.startAdapter(adapter);
      } else if ("stop".equals(action)) {
        adapterService.stopAdapter(adapter);
      } else {
        throw new BadRequestException(
          String.format("Invalid adapter action: %s. Possible actions: ['start', 'stop'].", action));
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SchedulerException e) {
      LOG.error("Scheduler error in namespace '{}' for adapter '{}' with action '{}'",
                namespaceId, adapterId, action, e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Retrieves the status of an adapter
   */
  @GET
  @Path("/adapters/{adapter-id}/status")
  public void getAdapterStatus(HttpRequest request, HttpResponder responder,
                               @PathParam("namespace-id") String namespaceId,
                               @PathParam("adapter-id") String adapterId) throws AdapterNotFoundException {

    Id.Adapter adapter = Id.Adapter.from(namespaceId, adapterId);
    AdapterStatus adapterStatus = adapterService.getAdapterStatus(adapter);
    responder.sendString(HttpResponseStatus.OK, adapterStatus.toString());
  }

  /**
   * Deletes an adapter
   */
  @DELETE
  @Path("/adapters/{adapter-id}")
  public void deleteAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterId) throws NotFoundException, SchedulerException {
    Id.Adapter adapter = Id.Adapter.from(namespaceId, adapterId);
    adapterService.removeAdapter(adapter);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  /**
   * Create an adapter.
   */
  @POST
  @Path("/adapters/{adapter-id}")
  public void createAdapter(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("adapter-id") String adapterName)
    throws SchedulerException, AdapterAlreadyExistsException, ScheduleAlreadyExistsException,
    IOException, BadRequestException, AdapterTypeNotFoundException {

    if (!namespaceAdmin.hasNamespace(Id.Namespace.from(namespaceId))) {
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           String.format("Create adapter failed - namespace '%s' does not exist.", namespaceId));
      return;
    }

    AdapterConfig config = parseBody(request, AdapterConfig.class);

    // Validate the adapter
    Id.AdapterType adapterType = Id.AdapterType.from(config.getType());
    AdapterTypeInfo adapterTypeInfo = adapterService.getAdapterTypeInfo(adapterType);
    AdapterSpecification spec = convertToSpec(adapterName, config, adapterTypeInfo);
    adapterService.createAdapter(namespaceId, spec);
    responder.sendString(HttpResponseStatus.OK, String.format("Adapter: %s is created", adapterName));
  }

  private AdapterSpecification convertToSpec(String name, AdapterConfig config, AdapterTypeInfo typeInfo) {
    Map<String, String> sourceProperties = Maps.newHashMap(typeInfo.getDefaultSourceProperties());
    if (config.source.properties != null) {
      sourceProperties.putAll(config.source.properties);
    }
    Set<Source> sources = ImmutableSet.of(new Source(config.source.name, typeInfo.getSourceType(), sourceProperties));
    Map<String, String> sinkProperties = Maps.newHashMap(typeInfo.getDefaultSinkProperties());
    if (config.sink.properties != null) {
      sinkProperties.putAll(config.sink.properties);
    }
    Set<Sink> sinks = ImmutableSet.of(
      new Sink(config.sink.name, typeInfo.getSinkType(), sinkProperties));
    Map<String, String> adapterProperties = Maps.newHashMap(typeInfo.getDefaultAdapterProperties());
    if (config.properties != null) {
      adapterProperties.putAll(config.properties);
    }
    return new AdapterSpecification(name, config.getType(), adapterProperties, sources, sinks);
  }

  /**
   *
   * @param request
   * @param responder
   * @param namespace
   * @param appId if null, use appId from application specification
   * @param archiveName
   * @return
   * @throws IOException
   * @throws NamespaceNotFoundException
   */
  private BodyConsumer deployApplication(final HttpRequest request, final HttpResponder responder,
                                         final Id.Namespace namespace, @Nullable final String appId,
                                         final String archiveName) throws IOException, NamespaceNotFoundException {
    if (!namespaceAdmin.hasNamespace(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }

    Location namespaceHomeLocation = locationFactory.create(namespace.getId());
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
                                 namespaceHomeLocation.toURI().getPath(), namespace.getId());
      LOG.error(msg);
      responder.sendString(HttpResponseStatus.NOT_FOUND, msg);
      return null;
    }


    if (archiveName == null || archiveName.isEmpty()) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present",
                           ImmutableMultimap.of(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE));
      return null;
    }

    // Store uploaded content to a local temp file
    String tempBase = String.format("%s/%s", configuration.get(Constants.CFG_LOCAL_DATA_DIR), namespace.getId());
    File tempDir = new File(tempBase, configuration.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    String appFabricDir = configuration.get(Constants.AppFabric.OUTPUT_DIR);
    // note: cannot create an appId subdirectory under the namespace directory here because appId could be null here
    final Location archive =
      namespaceHomeLocation.append(appFabricDir).append(Constants.ARCHIVE_DIR).append(archiveName);

    File tempFile = File.createTempFile("app-", ".jar", tempDir);
    LOG.debug("Creating temporary app jar for deploy at '{}'", tempFile.getAbsolutePath());
    return new AbstractBodyConsumer(tempFile) {
      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) {
        try {
          DeploymentInfo deploymentInfo = new DeploymentInfo(uploadedFile, archive);
          appLifecycleService.deploy(namespace, appId, deploymentInfo);
          responder.sendStatus(HttpResponseStatus.OK);
        } catch (Throwable t) {
          exceptionHandler.handle(t, request, responder);
        }
      }
    };
  }

  private static ApplicationRecord makeAppRecord(ApplicationSpecification appSpec) {
    return new ApplicationRecord("App", appSpec.getName(), appSpec.getName(), appSpec.getDescription());
  }

  private static List<ApplicationRecord> makeAppRecords(Iterable<ApplicationSpecification> appSpecs) {
    return Lists.newArrayList(Iterables.transform(appSpecs, new Function<ApplicationSpecification, ApplicationRecord>() {
      @Nullable
      @Override
      public ApplicationRecord apply(@Nullable ApplicationSpecification appSpec) {
        return makeAppRecord(appSpec);
      }
    }));
  }

}
