/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.data.format.RecordFormats;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.upload.ContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.LengthBasedContentWriterFactory;
import co.cask.cdap.data.stream.service.upload.StreamBodyConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.common.Threads;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST call to V3 stream APIs.
 *
 * TODO: Currently stream "dataset" is implementing old dataset API, hence not supporting multi-tenancy.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/streams")
public final class StreamHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StreamProperties.class, new StreamPropertiesAdapter())
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final CConfiguration cConf;
  private final StreamAdmin streamAdmin;
  private final MetricsCollector streamHandlerMetricsCollector;

  private final LoadingCache<Id.Namespace, MetricsCollector> streamMetricsCollectors;
  private final ConcurrentStreamWriter streamWriter;
  private final long batchBufferThreshold;
  private final StreamBodyConsumerFactory streamBodyConsumerFactory;
  private final AbstractNamespaceClient namespaceClient;

  // Executor for serving async enqueue requests
  private ExecutorService asyncExecutor;

  // TODO: Need to make the decision of whether this should be inside StreamAdmin or not.
  // Currently is here to align with the existing CDAP organization that dataset admin is not aware of MDS
  private final StreamMetaStore streamMetaStore;

  private final StreamWriterSizeCollector sizeCollector;

  @Inject
  public StreamHandler(CConfiguration cConf, Authenticator authenticator,
                       StreamCoordinatorClient streamCoordinatorClient, StreamAdmin streamAdmin,
                       StreamMetaStore streamMetaStore, StreamFileWriterFactory writerFactory,
                       final MetricsCollectionService metricsCollectionService,
                       StreamWriterSizeCollector sizeCollector,
                       AbstractNamespaceClient namespaceClient) {
    super(authenticator);
    this.cConf = cConf;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.sizeCollector = sizeCollector;
    this.batchBufferThreshold = cConf.getLong(Constants.Stream.BATCH_BUFFER_THRESHOLD);
    this.streamBodyConsumerFactory = new StreamBodyConsumerFactory();
    this.streamHandlerMetricsCollector = metricsCollectionService.getCollector(getStreamHandlerMetricsContext());
    streamMetricsCollectors = CacheBuilder.newBuilder()
      .build(new CacheLoader<Id.Namespace, MetricsCollector>() {
        @Override
        public MetricsCollector load(Id.Namespace namespaceId) {
          return metricsCollectionService.getCollector(getStreamMetricsContext(namespaceId));
        }
      });
    StreamMetricsCollectorFactory metricsCollectorFactory = createStreamMetricsCollectorFactory();
    this.streamWriter = new ConcurrentStreamWriter(streamCoordinatorClient, streamAdmin, streamMetaStore, writerFactory,
                                                   cConf.getInt(Constants.Stream.WORKER_THREADS),
                                                   metricsCollectorFactory);
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    int asyncWorkers = cConf.getInt(Constants.Stream.ASYNC_WORKER_THREADS);
    // The queue size config is size per worker, hence multiple by workers here
    int asyncQueueSize = cConf.getInt(Constants.Stream.ASYNC_QUEUE_SIZE) * asyncWorkers;

    // Creates a thread pool that will shrink inactive threads
    // Also, it limits how many tasks can get queue up to guard against out of memory if incoming requests are
    // coming too fast.
    // It uses the caller thread execution rejection policy so that it slows down request naturally by resorting
    // to sync enqueue (enqueue by caller thread is the same as sync enqueue)
    ThreadPoolExecutor executor = new ThreadPoolExecutor(asyncWorkers, asyncWorkers, 60, TimeUnit.SECONDS,
                                                         new ArrayBlockingQueue<Runnable>(asyncQueueSize),
                                                         Threads.createDaemonThreadFactory("async-exec-%d"),
                                                         createAsyncRejectedExecutionHandler());
    executor.allowCoreThreadTimeOut(true);
    asyncExecutor = executor;
  }

  @Override
  public void destroy(HandlerContext context) {
    Closeables.closeQuietly(streamWriter);
    asyncExecutor.shutdownNow();
  }

  @GET
  @Path("/{stream}")
  public void getInfo(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("stream") String stream) throws Exception {
    Id.Stream streamId = Id.Stream.from(namespaceId, stream);

    if (streamMetaStore.streamExists(streamId)) {
      StreamConfig streamConfig = streamAdmin.getConfig(streamId);
      StreamProperties streamProperties = new StreamProperties(streamConfig.getTTL(), streamConfig.getFormat(),
                                                               streamConfig.getNotificationThresholdMB());
      responder.sendJson(HttpResponseStatus.OK, streamProperties, StreamProperties.class, GSON);
    } else {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @PUT
  @Path("/{stream}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("stream") String stream) throws Exception {
    try {
      // Verify stream name
      Id.Stream streamId = Id.Stream.from(namespaceId, stream);

      // Check for namespace existence. Throws NotFoundException if namespace doesn't exist
      namespaceClient.get(namespaceId);

      // TODO: Modify the REST API to support custom configurations.
      streamAdmin.create(streamId);
      streamMetaStore.addStream(streamId);

      // TODO: For create successful, 201 Created should be returned instead of 200.
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      LOG.error("Failed to create stream {}", stream, e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (NotFoundException e) {
      LOG.error("Failed to create stream {}", stream, e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Failed to create stream {}", stream, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("/{stream}")
  public void enqueue(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("stream") String stream) throws Exception {
    Id.Stream streamId = Id.Stream.from(namespaceId, stream);

    try {
      streamWriter.enqueue(streamId, getHeaders(request, stream), request.getContent().toByteBuffer());
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    } catch (IOException e) {
      LOG.error("Failed to write to stream {}", stream, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  @POST
  @Path("/{stream}/async")
  public void asyncEnqueue(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId,
                           @PathParam("stream") String stream) throws Exception {
    Id.Stream streamId = Id.Stream.from(namespaceId, stream);
    // No need to copy the content buffer as we always uses a ChannelBufferFactory that won't reuse buffer.
    // See StreamHttpService
    streamWriter.asyncEnqueue(streamId, getHeaders(request, stream),
                              request.getContent().toByteBuffer(), asyncExecutor);
    responder.sendStatus(HttpResponseStatus.ACCEPTED);
  }

  @POST
  @Path("/{stream}/batch")
  public BodyConsumer batch(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId,
                            @PathParam("stream") String stream) throws Exception {
    Id.Stream streamId = Id.Stream.from(namespaceId, stream);

    if (!streamMetaStore.streamExists(streamId)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return null;
    }

    try {
      return streamBodyConsumerFactory.create(request, createContentWriterFactory(streamId, request));
    } catch (UnsupportedOperationException e) {
      responder.sendString(HttpResponseStatus.NOT_ACCEPTABLE, e.getMessage());
      return null;
    }
  }

  @POST
  @Path("/{stream}/truncate")
  public void truncate(HttpRequest request, HttpResponder responder,
                       @PathParam("namespace-id") String namespaceId,
                       @PathParam("stream") String stream) throws Exception {
    Id.Stream streamId = Id.Stream.from(namespaceId, stream);

    if (!streamMetaStore.streamExists(streamId)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
      return;
    }

    try {
      streamAdmin.truncate(streamId);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (IOException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exists");
    }
  }

  @PUT
  @Path("/{stream}/properties")
  public void setConfig(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId,
                        @PathParam("stream") String stream) throws Exception {
    Id.Stream streamId = Id.Stream.from(namespaceId, stream);

    if (!streamMetaStore.streamExists(streamId)) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Stream does not exist.");
      return;
    }

    StreamProperties properties = getAndValidateConfig(request, responder);
    // null is returned if the requested config is invalid. An appropriate response will have already been written
    // to the responder so we just need to return.
    if (properties == null) {
      return;
    }

    streamAdmin.updateConfig(streamId, properties);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private StreamMetricsCollectorFactory createStreamMetricsCollectorFactory() {
    return new StreamMetricsCollectorFactory() {
      @Override
      public StreamMetricsCollector createMetricsCollector(final Id.Stream streamId) {
        MetricsCollector streamMetricsCollector = streamMetricsCollectors.getUnchecked(streamId.getNamespace());
        final MetricsCollector childCollector =
          streamMetricsCollector.childCollector(Constants.Metrics.Tag.STREAM, streamId.getName());
        return new StreamMetricsCollector() {
          @Override
          public void emitMetrics(long bytesWritten, long eventsWritten) {
            if (bytesWritten > 0) {
              childCollector.increment("collect.bytes", bytesWritten);
              sizeCollector.received(streamId, bytesWritten);
            }
            if (eventsWritten > 0) {
              childCollector.increment("collect.events", eventsWritten);
            }
          }
        };
      }
    };
  }

  private Map<String, String> getStreamHandlerMetricsContext() {
    return ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                           Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                           Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME,
                           Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.Stream.CONTAINER_INSTANCE_ID, "0"));
  }

  private Map<String, String> getStreamMetricsContext(Id.Namespace namespaceId) {
    return ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, namespaceId.getId(),
                           Constants.Metrics.Tag.COMPONENT, Constants.Gateway.METRICS_CONTEXT,
                           Constants.Metrics.Tag.HANDLER, Constants.Gateway.STREAM_HANDLER_NAME,
                           Constants.Metrics.Tag.INSTANCE_ID, cConf.get(Constants.Stream.CONTAINER_INSTANCE_ID, "0"));
  }

  /**
   * Gets stream properties from the request. If there is request is invalid, response will be made and {@code null}
   * will be return.
   */
  private StreamProperties getAndValidateConfig(HttpRequest request, HttpResponder responder) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    StreamProperties properties;
    try {
      properties = GSON.fromJson(reader, StreamProperties.class);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid stream configuration. Please check that the " +
        "configuration is a valid JSON Object with a valid schema.");
      return null;
    }

    // Validate ttl
    Long ttl = properties.getTTL();
    if (ttl != null && ttl < 0) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "TTL value should be positive.");
      return null;
    }

    // Validate format
    FormatSpecification formatSpec = properties.getFormat();
    if (formatSpec != null) {
      String formatName = formatSpec.getName();
      if (formatName == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "A format name must be specified.");
        return null;
      }
      try {
        // if a format is given, make sure it is a valid format,
        // check that we can instantiate the format class
        RecordFormat<?, ?> format = RecordFormats.createInitializedFormat(formatSpec);
        // the request may contain a null schema, in which case the default schema of the format should be used.
        // create a new specification object that is guaranteed to have a non-null schema.
        formatSpec = new FormatSpecification(formatSpec.getName(),
                                                format.getSchema(), formatSpec.getSettings());
      } catch (UnsupportedTypeException e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             "Format " + formatName + " does not support the requested schema.");
        return null;
      } catch (Exception e) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             "Invalid format, unable to instantiate format " + formatName);
        return null;
      }
    }

    // Validate notification threshold
    Integer threshold = properties.getNotificationThresholdMB();
    if (threshold != null && threshold <= 0) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Threshold value should be greater than zero.");
      return null;
    }

    return new StreamProperties(ttl, formatSpec, threshold);
  }

  private RejectedExecutionHandler createAsyncRejectedExecutionHandler() {
    return new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (!executor.isShutdown()) {
          streamHandlerMetricsCollector.increment("collect.async.reject", 1);
          r.run();
        }
      }
    };
  }

  /**
   * Same as calling {@link #getHeaders(HttpRequest, String, ImmutableMap.Builder)} with a new builder.
   */
  private Map<String, String> getHeaders(HttpRequest request, String stream) {
    return getHeaders(request, stream, ImmutableMap.<String, String>builder());
  }

  /**
   * Extracts event headers from the HTTP request. Only HTTP headers that are prefixed with "{@code <stream-name>.}"
   * will be included. The result will be stored in an Immutable map built by the given builder.
   */
  private Map<String, String> getHeaders(HttpRequest request, String stream,
                                         ImmutableMap.Builder<String, String> builder) {
    // and transfer all other headers that are to be preserved
    String prefix = stream + ".";
    for (Map.Entry<String, String> header : request.getHeaders()) {
      if (header.getKey().startsWith(prefix)) {
        builder.put(header.getKey().substring(prefix.length()), header.getValue());
      }
    }
    return builder.build();
  }

  /**
   * Creates a {@link ContentWriterFactory} based on the request size. Used by the batch endpoint.
   */
  private ContentWriterFactory createContentWriterFactory(Id.Stream streamId, HttpRequest request) throws IOException {
    String contentType = HttpHeaders.getHeader(request, HttpHeaders.Names.CONTENT_TYPE, "");

    // The content-type is guaranteed to be non-empty, otherwise the batch request itself will fail.
    Map<String, String> headers = getHeaders(request, streamId.getName(),
                                             ImmutableMap.<String, String>builder().put("content.type", contentType));

    StreamConfig config = streamAdmin.getConfig(streamId);
    return new LengthBasedContentWriterFactory(config, streamWriter, headers, batchBufferThreshold);
  }

  /**
   *  Adapter class for {@link StreamProperties}. Its main purpose is to transform
   *  the unit of TTL, which is second in JSON, but millisecond in the StreamProperties object.
   */
  private static final class StreamPropertiesAdapter implements JsonSerializer<StreamProperties>,
                                                                JsonDeserializer<StreamProperties> {
    @Override
    public JsonElement serialize(StreamProperties src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject json = new JsonObject();
      if (src.getTTL() != null) {
        json.addProperty("ttl", TimeUnit.MILLISECONDS.toSeconds(src.getTTL()));
      }
      if (src.getFormat() != null) {
        json.add("format", context.serialize(src.getFormat(), FormatSpecification.class));
      }
      if (src.getNotificationThresholdMB() != null) {
        json.addProperty("notification.threshold.mb", src.getNotificationThresholdMB());
      }
      return json;
    }

    @Override
    public StreamProperties deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      Long ttl = jsonObj.has("ttl") ? TimeUnit.SECONDS.toMillis(jsonObj.get("ttl").getAsLong()) : null;
      FormatSpecification format = null;
      if (jsonObj.has("format")) {
        format = context.deserialize(jsonObj.get("format"), FormatSpecification.class);
      }
      Integer threshold = jsonObj.has("notification.threshold.mb") ?
        jsonObj.get("notification.threshold.mb").getAsInt() :
        null;
      return new StreamProperties(ttl, format, threshold);
    }
  }
}
