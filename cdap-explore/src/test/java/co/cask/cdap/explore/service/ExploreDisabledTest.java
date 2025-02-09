/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.service.datasets.KeyStructValueTableDefinition;
import co.cask.cdap.explore.service.datasets.NotRecordScannableTableDefinition;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.NoOpNotificationFeedManager;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Test deployment behavior when explore module is disabled.
 */
public class ExploreDisabledTest {
  private static final Id.Namespace namespaceId = Id.Namespace.from("myspace");

  private static TransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetOpExecutor dsOpExecutor;
  private static DatasetService datasetService;
  private static ExploreClient exploreClient;
  private static NamespacedLocationFactory namespacedLocationFactory;

  @BeforeClass
  public static void start() throws Exception {
    Injector injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(TransactionManager.class);
    transactionManager.startAndWait();

    dsOpExecutor = injector.getInstance(DatasetOpExecutor.class);
    dsOpExecutor.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    exploreClient = injector.getInstance(DiscoveryExploreClient.class);
    Assert.assertFalse(exploreClient.isServiceAvailable());

    datasetFramework = injector.getInstance(DatasetFramework.class);

    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);

    // This happens when you create a namespace. However, simulating that scenario by creating a directory here instead.
    Locations.mkdirsIfNotExists(namespacedLocationFactory.get(namespaceId));
  }

  @AfterClass
  public static void stop() throws Exception {
    Locations.deleteQuietly(namespacedLocationFactory.get(namespaceId));
    exploreClient.close();
    datasetService.stopAndWait();
    dsOpExecutor.stopAndWait();
    transactionManager.stopAndWait();
  }

  @Test
  public void testDeployRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exception being thrown
    Id.DatasetModule module1 = Id.DatasetModule.from(namespaceId, "module1");
    Id.DatasetInstance instance1 = Id.DatasetInstance.from(namespaceId, "table1");
    datasetFramework.addModule(module1, new KeyStructValueTableDefinition.KeyStructValueTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyStructValueTable", instance1, DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table =
      datasetFramework.getDataset(instance1, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    KeyStructValueTableDefinition.KeyValue.Value value1 =
        new KeyStructValueTableDefinition.KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyStructValueTableDefinition.KeyValue.Value value2 =
        new KeyStructValueTableDefinition.KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals(value1, table.get("1"));

    datasetFramework.deleteInstance(instance1);
    datasetFramework.deleteModule(module1);
  }

  @Test
  public void testDeployNotRecordScannable() throws Exception {
    // Try to deploy a dataset that is not record scannable, when explore is enabled.
    // This should be processed with no exceptionbeing thrown
    Id.DatasetModule module2 = Id.DatasetModule.from(namespaceId, "module2");
    Id.DatasetInstance instance2 = Id.DatasetInstance.from(namespaceId, "table1");
    datasetFramework.addModule(module2, new NotRecordScannableTableDefinition.NotRecordScannableTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("NotRecordScannableTableDef", instance2, DatasetProperties.EMPTY);

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    NotRecordScannableTableDefinition.KeyValueTable table =
      datasetFramework.getDataset(instance2, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    table.write("key1", "value1");
    table.write("key2", "value2");
    Assert.assertEquals("value1", new String(table.read("key1")));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals("value1", new String(table.read("key1")));

    datasetFramework.deleteInstance(instance2);
    datasetFramework.deleteModule(module2);
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());
    configuration.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);
    configuration.set(Constants.Explore.LOCAL_DATA_DIR,
                      new File(System.getProperty("java.io.tmpdir"), "hive").getAbsolutePath());

    return ImmutableList.of(
        new ConfigModule(configuration, hConf),
        new IOModule(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new DataFabricModules().getInMemoryModules(),
        new DataSetsModules().getStandaloneModules(),
        new DataSetServiceModules().getInMemoryModules(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new AuthModule(),
        new ExploreRuntimeModule().getInMemoryModules(),
        new ExploreClientModule(),
        new StreamAdminModules().getInMemoryModules(),
        new NotificationServiceRuntimeModule().getInMemoryModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(NotificationFeedManager.class).to(NoOpNotificationFeedManager.class);
            install(new FactoryModuleBuilder()
                      .implement(Store.class, DefaultStore.class)
                      .build(StoreFactory.class)
            );
          }
        }
    );
  }
}
