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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Admin for managing namespaces
 */
public final class DefaultNamespaceAdmin implements NamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);
  private static final String NAMESPACE_ELEMENT_TYPE = "Namespace";

  private final Store store;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;
  private final DatasetFramework dsFramework;
  private final ProgramRuntimeService runtimeService;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final MetricStore metricStore;
  private final Scheduler scheduler;

  @Inject
  public DefaultNamespaceAdmin(Store store, PreferencesStore preferencesStore,
                               DashboardStore dashboardStore, DatasetFramework dsFramework,
                               ProgramRuntimeService runtimeService, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                               MetricStore metricStore, Scheduler scheduler) {
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = store;
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.runtimeService = runtimeService;
    this.scheduler = scheduler;
    this.metricStore = metricStore;
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  public List<NamespaceMeta> listNamespaces() {
    return store.listNamespaces();
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NotFoundException if the requested namespace is not found
   */
  public NamespaceMeta getNamespace(Id.Namespace namespaceId) throws NotFoundException {
    NamespaceMeta ns = store.getNamespace(namespaceId);
    if (ns == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
    return ns;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  public boolean hasNamespace(Id.Namespace namespaceId) {
    try {
      getNamespace(namespaceId);
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws AlreadyExistsException if the specified namespace already exists
   */
  public void createNamespace(NamespaceMeta metadata) throws NamespaceCannotBeCreatedException, AlreadyExistsException {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    if (hasNamespace(Id.Namespace.from(metadata.getName()))) {
      throw new AlreadyExistsException(NAMESPACE_ELEMENT_TYPE, metadata.getName());
    }

    try {
      dsFramework.createNamespace(Id.Namespace.from(metadata.getName()));
    } catch (DatasetManagementException e) {
      throw new NamespaceCannotBeCreatedException(metadata.getName(), e);
    }

    store.createNamespace(metadata);
  }

  /**
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NamespaceCannotBeDeletedException if the specified namespace cannot be deleted
   * @throws NotFoundException if the specified namespace does not exist
   */
  public void deleteNamespace(final Id.Namespace namespaceId)
    throws NamespaceCannotBeDeletedException, NotFoundException {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!hasNamespace(namespaceId)) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }

    if (areProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(),
                                                  "Some programs are currently running in namespace " + namespaceId);
    }

    LOG.info("Deleting namespace '{}'.", namespaceId);
    try {
      // Delete Preferences associated with this namespace
      preferencesStore.deleteProperties(namespaceId.getId());
      // Delete all dashboards associated with this namespace
      dashboardStore.delete(namespaceId.getId());
      // Delete datasets and modules
      dsFramework.deleteAllInstances(namespaceId);
      dsFramework.deleteAllModules(namespaceId);
      // Delete queues and streams data
      queueAdmin.dropAllInNamespace(namespaceId.getId());
      streamAdmin.dropAllInNamespace(namespaceId);
      // Delete all the schedules
      scheduler.deleteAllSchedules(namespaceId);
      // Delete all meta data
      store.removeAll(namespaceId);

      deleteMetrics(namespaceId);

      // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
      // create default namespace, and hence deleting it may cause undeterministic behavior.
      // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
      // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
      // This condition is only required to support the v2 unrecoverable reset API, since that uses NamespaceAdmin
      // directly. If you go through the Namespace delete REST API, this condition will already be met, since that
      // disallows deletion of reserved namespaces altogether.
      // TODO: Remove this check when the v2 unrecoverable reset API is removed
      if (!Constants.DEFAULT_NAMESPACE_ID.equals(namespaceId)) {
        // Delete namespace in storage providers
        dsFramework.deleteNamespace(namespaceId);
        // Finally delete namespace from MDS
        store.deleteNamespace(namespaceId);
      }
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    }
    LOG.info("All data for namespace '{}' deleted.", namespaceId);
  }

  private void deleteMetrics(Id.Namespace namespaceId) throws Exception {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId.getId());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, null, tags);
    metricStore.delete(deleteQuery);
  }

  @Override
  public void deleteDatasets(Id.Namespace namespaceId)
    throws NotFoundException, NamespaceCannotBeDeletedException {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!hasNamespace(namespaceId)) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }

    if (areProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(),
                                                  "Some programs are currently running in namespace " + namespaceId);
    }

    LOG.info("Deleting data in namespace '{}'.", namespaceId);
    try {
      dsFramework.deleteAllInstances(namespaceId);
    } catch (DatasetManagementException e) {
      LOG.warn("Error while deleting data in namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    } catch (IOException e) {
      LOG.warn("Error while deleting data in namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId.getId(), e);
    }
    LOG.info("Deleted data in namespace '{}'.", namespaceId);
  }

  public void updateProperties(Id.Namespace namespaceId, NamespaceMeta namespaceMeta) throws NotFoundException {
    if (store.getNamespace(namespaceId) == null) {
      throw new NotFoundException(NAMESPACE_ELEMENT_TYPE, namespaceId.getId());
    }
    NamespaceMeta metadata = store.getNamespace(namespaceId);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder(metadata);

    if (namespaceMeta.getDescription() != null) {
      builder.setDescription(namespaceMeta.getDescription());
    }

    if (namespaceMeta.getName() != null) {
      builder.setName(namespaceMeta.getName());
    }

    NamespaceConfig config = namespaceMeta.getConfig();

    if (config != null && config.getSchedulerQueueName() != null && !config.getSchedulerQueueName().isEmpty()) {
      builder.setSchedulerQueueName(config.getSchedulerQueueName());
    }

    store.updateNamespace(builder.build());
  }

  private boolean areProgramsRunning(final Id.Namespace namespaceId) {
    return runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program program) {
        return program.getNamespaceId().equals(namespaceId.getId());
      }
    }, ProgramType.values());
  }
}
