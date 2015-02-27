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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.common.exception.AdapterNotFoundException;
import co.cask.cdap.common.exception.ApplicationNotFoundException;
import co.cask.cdap.common.exception.NamespaceAlreadyExistsException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.adapter.AdapterAlreadyExistsException;
import co.cask.cdap.internal.app.runtime.adapter.AdapterStatus;
import co.cask.cdap.proto.AdapterSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Store for application metadata
 */
public class AppMetadataStore extends MetadataStoreDataset {
  private static final Logger LOG = LoggerFactory.getLogger(AppMetadataStore.class);

  private static final Gson GSON;

  static {
    GsonBuilder builder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(builder);
    GSON = builder.create();
  }

  private static final String TYPE_APP_META = "appMeta";
  private static final String TYPE_STREAM = "stream";
  private static final String TYPE_RUN_RECORD_STARTED = "runRecordStarted";
  private static final String TYPE_RUN_RECORD_COMPLETED = "runRecordCompleted";
  private static final String TYPE_PROGRAM_ARGS = "programArgs";
  private static final String TYPE_NAMESPACE = "namespace";
  private static final String TYPE_ADAPTER = "adapter";

  public AppMetadataStore(Table table) {
    super(table);
  }

  @Override
  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  @Override
  protected <T> T deserialize(byte[] serialized, Class<T> classOfT) {
    return GSON.fromJson(Bytes.toString(serialized), classOfT);
  }

  // ------------- Application API ---------------

  public ApplicationMeta getApplication(Id.Application app) throws ApplicationNotFoundException {
    ApplicationMeta appMeta = get(getAppKey(app), ApplicationMeta.class);
    if (appMeta == null) {
      throw new ApplicationNotFoundException(app);
    }
    return appMeta;
  }

  public List<ApplicationMeta> getAllApplications(Id.Namespace namespace) throws NamespaceNotFoundException {
    List<ApplicationMeta> list = list(getAppsKey(namespace), ApplicationMeta.class);
    if (list == null) {
      throw new NamespaceNotFoundException(namespace);
    }
    return list;
  }

  public void writeApplication(Id.Application app, ApplicationSpecification spec,
                               String archiveLocation) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    write(getAppKey(app), new ApplicationMeta(app.getId(), spec, archiveLocation));
  }

  public void deleteApplication(Id.Application app) throws NamespaceNotFoundException, ApplicationNotFoundException {
    assertNamespaceExists(app.getNamespace());
    assertAppExists(app);
    deleteAll(getAppKey(app));
  }

  public void deleteApplications(Id.Namespace namespace) throws NamespaceNotFoundException {
    assertNamespaceExists(namespace);
    deleteAll(getAppsKey(namespace));
  }

  // todo: do we need appId? may be use from appSpec?
  public void updateAppSpec(Id.Application app, ApplicationSpecification spec) {
    // NOTE: we use Gson underneath to do serde, as it doesn't serialize inner classes (which we use everywhere for
    //       specs - see forwarding specs), we want to wrap spec with DefaultApplicationSpecification
    spec = DefaultApplicationSpecification.from(spec);
    LOG.trace("App spec to be updated: id: {}: spec: {}", app.getId(), GSON.toJson(spec));
    MDSKey key = getAppKey(app);

    ApplicationMeta existing = get(key, ApplicationMeta.class);
    if (existing == null) {
      String msg = String.format("No meta for namespace %s app %s exists", app.getNamespaceId(), app.getId());
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    LOG.trace("Application exists in mds: id: {}, spec: {}", existing);
    ApplicationMeta updated = ApplicationMeta.updateSpec(existing, spec);
    write(key, updated);

    for (StreamSpecification streamSpec : spec.getStreams().values()) {
      writeStream(app.getNamespace(), streamSpec);
    }
  }

  private MDSKey getAppKey(Id.Application app) {
    return new MDSKey.Builder().add(TYPE_APP_META, app.getNamespaceId(), app.getId()).build();
  }

  private MDSKey getAppsKey(Id.Namespace namespace) {
    return new MDSKey.Builder().add(TYPE_APP_META, namespace.getId()).build();
  }

  private void assertAppExists(Id.Application application) throws ApplicationNotFoundException {
    if (!appExists(application)) {
      throw new ApplicationNotFoundException(application);
    }
  }

  private void assertAppNotExists(Id.Adapter adapter) throws AdapterAlreadyExistsException {
    if (!adapterExists(adapter)) {
      throw new AdapterAlreadyExistsException(adapter);
    }
  }

  private boolean appExists(Id.Application application) {
    return exists(getAppKey(application));
  }

  private ApplicationMeta getAppMeta(Id.Application app) throws ApplicationNotFoundException {
    ApplicationMeta appMeta = get(getAppKey(app), ApplicationMeta.class);
    if (appMeta == null) {
      throw new ApplicationNotFoundException(app);
    }
    return appMeta;
  }

  public void recordProgramStart(String namespaceId, String appId, String programId, String pid, long startTs) {
      write(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, programId, pid).build(),
            new RunRecord(pid, startTs, null, ProgramRunStatus.RUNNING));
  }

  public void recordProgramStop(String namespaceId, String appId, String programId,
                                String pid, long stopTs, ProgramController.State endStatus) {
    MDSKey key = new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, programId, pid).build();
    RunRecord started = get(key, RunRecord.class);
    if (started == null) {
      String msg = String.format("No meta for started run record for namespace %s app %s program %s pid %s exists",
                                 namespaceId, appId, programId, pid);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    deleteAll(key);

    key = new MDSKey.Builder()
      .add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId, programId)
      .add(getInvertedTsKeyPart(started.getStartTs()))
      .add(pid).build();
    write(key, new RunRecord(started, stopTs, endStatus.getRunStatus()));
  }

  public List<RunRecord> getRuns(String namespaceId, String appId, String programId,
                                 ProgramRunStatus status,
                                 long startTime, long endTime, int limit) {
    if (status.equals(ProgramRunStatus.ALL)) {
      List<RunRecord> resultRecords = Lists.newArrayList();
      resultRecords.addAll(getActiveRuns(namespaceId, appId, programId, startTime, endTime, limit));
      resultRecords.addAll(getHistoricalRuns(namespaceId, appId, programId, status, startTime, endTime, limit));
      return resultRecords;
    } else if (status.equals(ProgramRunStatus.RUNNING)) {
      return getActiveRuns(namespaceId, appId, programId, startTime, endTime, limit);
    } else {
      return getHistoricalRuns(namespaceId, appId, programId, status, startTime, endTime, limit);
    }
  }

  private List<RunRecord> getActiveRuns(String namespaceId, String appId, String programId,
                                        final long startTime, final long endTime, int limit) {
    MDSKey activeKey = new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId, programId).build();
    MDSKey start = new MDSKey.Builder(activeKey).add(getInvertedTsKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(activeKey).add(getInvertedTsKeyPart(startTime)).build();
    return list(start, stop, RunRecord.class, limit, Predicates.<RunRecord>alwaysTrue());
  }

  private List<RunRecord> getHistoricalRuns(String namespaceId, String appId, String programId,
                                            ProgramRunStatus status,
                                            final long startTime, final long endTime, int limit) {
    MDSKey historyKey = new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId, programId).build();
    MDSKey start = new MDSKey.Builder(historyKey).add(getInvertedTsKeyPart(endTime)).build();
    MDSKey stop = new MDSKey.Builder(historyKey).add(getInvertedTsKeyPart(startTime)).build();
    if (status.equals(ProgramRunStatus.ALL)) {
      //return all records (successful and failed)
      return list(start, stop, RunRecord.class, limit, Predicates.<RunRecord>alwaysTrue());
    }
    if (status.equals(ProgramRunStatus.COMPLETED)) {
      return list(start, stop, RunRecord.class, limit, getPredicate(ProgramController.State.STOPPED));
    }
    return list(start, stop, RunRecord.class, limit, getPredicate(ProgramController.State.ERROR));
  }

  private Predicate<RunRecord> getPredicate(final ProgramController.State state) {
    return new Predicate<RunRecord>() {
      @Override
      public boolean apply(RunRecord record) {
        return record.getStatus().equals(state.getRunStatus());
      }
    };
  }

  private long getInvertedTsKeyPart(long endTime) {
    return Long.MAX_VALUE - endTime;
  }

  public void writeStream(Id.Namespace namespace, StreamSpecification spec) {
    write(getStreamsKey(namespace).build(), spec);
  }

  private MDSKey getNamespaceKey(Id.Namespace namespace) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    builder.add(namespace.getId());
    return builder.build();
  }

  private MDSKey getNamespacesKey() {
    return new MDSKey.Builder().add(TYPE_NAMESPACE).build();
  }


  public StreamSpecification getStream(String namespaceId, String name) {
    return get(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, name).build(), StreamSpecification.class);
  }

  public List<StreamSpecification> getAllStreams(String namespaceId) {
    return list(new MDSKey.Builder().add(TYPE_STREAM, namespaceId).build(), StreamSpecification.class);
  }

  public void deleteAllStreams(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_STREAM, namespaceId).build());
  }

  public void deleteStream(String namespaceId, String name) {
    deleteAll(new MDSKey.Builder().add(TYPE_STREAM, namespaceId, name).build());
  }

  public void writeProgramArgs(String namespaceId, String appId, String programName, Map<String, String> args) {
    write(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId, programName).build(), new ProgramArgs(args));
  }

  public ProgramArgs getProgramArgs(String namespaceId, String appId, String programName) {
    return get(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId, programName).build(), ProgramArgs.class);
  }

  public void deleteProgramArgs(String namespaceId, String appId, String programName) {
    deleteAll(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId, programName).build());
  }

  public void deleteProgramArgs(String namespaceId, String appId) {
    deleteAll(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId, appId).build());
  }

  public void deleteProgramArgs(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_PROGRAM_ARGS, namespaceId).build());
  }

  public void deleteProgramHistory(String namespaceId, String appId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId, appId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId, appId).build());
  }

  public void deleteProgramHistory(String namespaceId) {
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_STARTED, namespaceId).build());
    deleteAll(new MDSKey.Builder().add(TYPE_RUN_RECORD_COMPLETED, namespaceId).build());
  }

  public void createNamespace(NamespaceMeta metadata) throws NamespaceAlreadyExistsException {
    Id.Namespace namespace = Id.Namespace.from(metadata.getId());
    assertNamespaceNotExists(namespace);
    write(getNamespaceKey(namespace), metadata);
  }

  public NamespaceMeta getNamespace(Id.Namespace namespace) throws NamespaceNotFoundException {
    NamespaceMeta namespaceMeta = get(getNamespaceKey(namespace), NamespaceMeta.class);
    if (namespaceMeta == null) {
      throw new NamespaceNotFoundException(namespace);
    }
    return namespaceMeta;
  }

  public void deleteNamespace(Id.Namespace id) throws NamespaceNotFoundException {
    assertNamespaceExists(id);
    deleteAll(getNamespaceKey(id));
  }

  public List<NamespaceMeta> listNamespaces() {
    return list(getNamespacesKey(), NamespaceMeta.class);
  }

  public void createAdapter(Id.Adapter adapter, AdapterSpecification adapterSpec,
                            AdapterStatus adapterStatus) throws AdapterAlreadyExistsException {
    assertAdapterNotExists(adapter);
    write(getAdapterKey(adapter), new AdapterMeta(adapterSpec, adapterStatus));
  }

  public void updateAdapter(Id.Adapter adapter, AdapterSpecification adapterSpec,
                            AdapterStatus adapterStatus) throws AdapterNotFoundException {
    assertAdapterExists(adapter);
    write(getAdapterKey(adapter), new AdapterMeta(adapterSpec, adapterStatus));
  }

  public AdapterSpecification getAdapterSpec(Id.Adapter adapter) throws AdapterNotFoundException {
    AdapterMeta adapterMeta = getAdapterMeta(adapter);
    return adapterMeta.getSpec();
  }

  public AdapterStatus getAdapterStatus(Id.Adapter adapter) throws AdapterNotFoundException {
    AdapterMeta adapterMeta = getAdapterMeta(adapter);
    if (adapterMeta == null) {
      throw new AdapterNotFoundException(adapter);
    }
    return adapterMeta.getStatus();
  }

  @Nullable
  public AdapterStatus updateAdapterStatus(Id.Adapter adapter, AdapterStatus status) throws AdapterNotFoundException {
    AdapterMeta adapterMeta = getAdapterMeta(adapter);
    AdapterStatus previousStatus = adapterMeta.getStatus();
    AdapterSpecification adapterSpec = getAdapterSpec(adapter);
    updateAdapter(adapter, adapterSpec, status);
    return previousStatus;
  }

  public List<AdapterSpecification> getAllAdapters(Id.Namespace namespace) throws NamespaceNotFoundException {
    assertNamespaceExists(namespace);
    List<AdapterSpecification> adapterSpecs = Lists.newArrayList();
    List<AdapterMeta> adapterMetas = list(getAdaptersKey(namespace), AdapterMeta.class);
    for (AdapterMeta adapterMeta : adapterMetas) {
      adapterSpecs.add(adapterMeta.getSpec());
    }
    return adapterSpecs;
  }

  // ------------- Namespace helpers ---------------

  private MDSKey getNamespaceKey(Id.Namespace namespace) {
    MDSKey.Builder builder = new MDSKey.Builder().add(TYPE_NAMESPACE);
    builder.add(namespace.getId());
    return builder.build();
  }

  private MDSKey getNamespacesKey() {
    return new MDSKey.Builder().add(TYPE_NAMESPACE).build();
  }

  private void assertNamespaceExists(Id.Namespace namespace) throws NamespaceNotFoundException {
    if (!namespaceExists(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }
  }

  private void assertNamespaceNotExists(Id.Namespace namespace) throws NamespaceAlreadyExistsException {
    if (namespaceExists(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  private boolean namespaceExists(Id.Namespace namespace) {
    return exists(getNamespaceKey(namespace));
  }

  // ------------- Adapter helpers ---------------

  private MDSKey getAdaptersKey(Id.Namespace namespace) {
    return new MDSKey.Builder().add(TYPE_ADAPTER, namespace.getId()).build();
  }

  private MDSKey getAdapterKey(Id.Adapter adapter) {
    return new MDSKey.Builder().add(TYPE_ADAPTER, adapter.getNamespaceId(), adapter.getId()).build();
  }

  public void deleteAdapter(Id.Adapter adapter) throws AdapterNotFoundException {
    assertAdapterExists(adapter);
    deleteAll(getAdapterKey(adapter));
  }

  public void deleteAllAdapters(Id.Namespace namespace) throws NamespaceNotFoundException {
    assertNamespaceExists(namespace);
    deleteAll(getAdaptersKey(namespace));
  }

  private void assertAdapterExists(Id.Adapter adapter) throws AdapterNotFoundException {
    if (!adapterExists(adapter)) {
      throw new AdapterNotFoundException(adapter);
    }
  }

  private void assertAdapterNotExists(Id.Adapter adapter) throws AdapterAlreadyExistsException {
    if (!adapterExists(adapter)) {
      throw new AdapterAlreadyExistsException(adapter);
    }
  }

  private boolean adapterExists(Id.Adapter adapter) {
    return exists(getAdapterKey(adapter));
  }

  private AdapterMeta getAdapterMeta(Id.Adapter adapter) throws AdapterNotFoundException {
    AdapterMeta adapterMeta = get(getAdapterKey(adapter), AdapterMeta.class);
    if (adapterMeta == null) {
      throw new AdapterNotFoundException(adapter);
    }
    return adapterMeta;
  }
}
