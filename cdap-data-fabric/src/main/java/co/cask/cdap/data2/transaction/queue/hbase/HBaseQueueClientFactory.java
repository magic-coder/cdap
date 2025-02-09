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
package co.cask.cdap.data2.transaction.queue.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.ForwardingTransactionAware;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.data2.transaction.queue.QueueConstants.QueueType;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutor.Subroutine;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Factory for creating HBase queue producer and consumer instances.
 */
public class HBaseQueueClientFactory implements QueueClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseQueueClientFactory.class);

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final HBaseQueueAdmin queueAdmin;
  private final HBaseStreamAdmin streamAdmin;
  private final HBaseQueueUtil queueUtil;
  private final HBaseTableUtil hBaseTableUtil;
  private final TransactionExecutorFactory txExecutorFactory;

  @Inject
  public HBaseQueueClientFactory(CConfiguration cConf, Configuration hConf, HBaseTableUtil hBaseTableUtil,
                                 QueueAdmin queueAdmin, HBaseStreamAdmin streamAdmin,
                                 TransactionExecutorFactory txExecutorFactory) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.queueAdmin = (HBaseQueueAdmin) queueAdmin;
    this.streamAdmin = streamAdmin;
    this.queueUtil = new HBaseQueueUtilFactory().get();
    this.hBaseTableUtil = hBaseTableUtil;
    this.txExecutorFactory = txExecutorFactory;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    HBaseQueueAdmin admin = ensureTableExists(queueName);
    try {
      final HBaseConsumerStateStore stateStore = admin.getConsumerStateStore(queueName);
      final List<ConsumerGroupConfig> groupConfigs = Lists.newArrayList();
      Transactions.createTransactionExecutor(txExecutorFactory, stateStore).execute(new Subroutine() {
        @Override
        public void apply() throws Exception {
          stateStore.getLatestConsumerGroups(groupConfigs);
        }
      });
      Preconditions.checkState(!groupConfigs.isEmpty(), "Missing consumer group information for queue %s", queueName);
      return createProducer(admin, queueName, queueAdmin.getType(),
                            queueMetrics, new ShardedHBaseQueueStrategy(), groupConfigs);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      throw new IOException(e);
    }
  }

  @Override
  public QueueConsumer createConsumer(final QueueName queueName,
                                      final ConsumerConfig consumerConfig, int numGroups) throws IOException {
    final HBaseQueueAdmin admin = ensureTableExists(queueName);
    try {
      final long groupId = consumerConfig.getGroupId();

      // A callback for create a list of HBaseQueueConsumer
      // based on the current queue consumer state of the given group
      Callable<List<HBaseQueueConsumer>> consumerCreator = new Callable<List<HBaseQueueConsumer>>() {

        @Override
        public List<HBaseQueueConsumer> call() throws Exception {
          List<HBaseConsumerState> states;
          final HBaseConsumerStateStore stateStore = admin.getConsumerStateStore(queueName);
          try {
            TransactionExecutor txExecutor = Transactions.createTransactionExecutor(txExecutorFactory, stateStore);

            // Find all consumer states for consumers that need to be created based on current state
            states = txExecutor.execute(new Callable<List<HBaseConsumerState>>() {
              @Override
              public List<HBaseConsumerState> call() throws Exception {
                List<HBaseConsumerState> consumerStates = Lists.newArrayList();

                HBaseConsumerState state = stateStore.getState(groupId, consumerConfig.getInstanceId());
                if (state.getPreviousBarrier() == null) {
                  // Old HBase consumer (Salted based, not sharded)
                  consumerStates.add(state);
                  return consumerStates;
                }

                // Find the smallest start barrier that has something to consume for this instance.
                // It should always exists since we assume the queue is configured before this method is called
                List<QueueBarrier> queueBarriers = stateStore.getAllBarriers(groupId);
                if (queueBarriers.isEmpty()) {
                  throw new IllegalStateException(
                    String.format("No consumer information available. Queue: %s, GroupId: %d, InstanceId: %d",
                                  queueName, groupId, consumerConfig.getInstanceId()));
                }
                QueueBarrier startBarrier = Iterables.find(Lists.reverse(queueBarriers), new Predicate<QueueBarrier>() {
                  @Override
                  public boolean apply(QueueBarrier barrier) {
                    return barrier.getGroupConfig().getGroupSize() > consumerConfig.getInstanceId()
                            && stateStore.isAllConsumed(consumerConfig, barrier.getStartRow());
                  }
                }, queueBarriers.get(0));

                int groupSize = startBarrier.getGroupConfig().getGroupSize();
                for (int i = consumerConfig.getInstanceId(); i < groupSize; i += consumerConfig.getGroupSize()) {
                  consumerStates.add(stateStore.getState(groupId, i));
                }
                return consumerStates;
              }
            });

          } finally {
            Closeables.closeQuietly(stateStore);
          }

          List<HBaseQueueConsumer> consumers = Lists.newArrayList();
          for (HBaseConsumerState state : states) {
            QueueType queueType = (state.getPreviousBarrier() == null) ? QueueType.QUEUE : QueueType.SHARDED_QUEUE;
            HBaseQueueStrategy strategy = (state.getPreviousBarrier() == null) ? new SaltedHBaseQueueStrategy()
                                                                                 : new ShardedHBaseQueueStrategy();
            HTable hTable = createHTable(admin.getDataTableId(queueName, queueType));
            consumers.add(queueUtil.getQueueConsumer(cConf, hTable, queueName, state,
                                                     admin.getConsumerStateStore(queueName),
                                                     strategy));
          }
          return consumers;
        }
      };

      return new SmartQueueConsumer(queueName, consumerConfig, consumerCreator);
    } catch (Exception e) {
      // If there is exception, nothing much can be done here besides propagating
      Throwables.propagateIfPossible(e);
      throw new IOException(e);
    }
  }

  /**
   * Creates a producer for the given queue.
   */
  @VisibleForTesting
  HBaseQueueProducer createProducer(HBaseQueueAdmin admin, QueueName queueName, QueueConstants.QueueType queueType,
                                    QueueMetrics queueMetrics, HBaseQueueStrategy queueStrategy,
                                    Iterable<? extends ConsumerGroupConfig> groupConfigs) throws IOException {
    return new HBaseQueueProducer(createHTable(admin.getDataTableId(queueName, queueType)),
                                  queueName, queueMetrics, queueStrategy, groupConfigs);
  }

  /**
   * Helper method to select the queue or stream admin, and to ensure it's table exists.
   * @param queueName name of the queue to be opened.
   * @return the queue admin for that queue.
   * @throws IOException
   */
  private HBaseQueueAdmin ensureTableExists(QueueName queueName) throws IOException {
    HBaseQueueAdmin admin = queueName.isStream() ? streamAdmin : queueAdmin;
    try {
      if (!admin.exists(queueName)) {
        admin.create(queueName);
      }
    } catch (Exception e) {
      throw new IOException("Failed to open table " + admin.getDataTableId(queueName), e);
    }
    return admin;
  }

  private HTable createHTable(TableId tableId) throws IOException {
    HTable consumerTable = hBaseTableUtil.createHTable(hConf, tableId);
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return consumerTable;
  }

  /**
   * A {@link QueueConsumer} that delegates to a list of consumers sequentially. It also has logic to renew
   * the consumers list when all existing consumers has consumed everything in the current queue barrier.
   */
  private final class SmartQueueConsumer extends ForwardingTransactionAware implements QueueConsumer {

    private final QueueName queueName;
    private final ConsumerConfig consumerConfig;
    private final Callable<? extends Iterable<HBaseQueueConsumer>> consumerCreator;
    private final Deque<HBaseQueueConsumer> consumers;

    private SmartQueueConsumer(QueueName queueName, ConsumerConfig consumerConfig,
                               Callable<? extends Iterable<HBaseQueueConsumer>> consumerCreator) throws Exception {
      this.queueName = queueName;
      this.consumerConfig = consumerConfig;
      this.consumers = Lists.newLinkedList(consumerCreator.call());
      this.consumerCreator = consumerCreator;
    }

    @Override
    protected TransactionAware delegate() {
      return consumers.peek();
    }

    @Override
    public QueueName getQueueName() {
      return queueName;
    }

    @Override
    public ConsumerConfig getConfig() {
      return consumerConfig;
    }

    @Override
    public DequeueResult<byte[]> dequeue() throws IOException {
      return dequeue(1);
    }

    @Override
    public DequeueResult<byte[]> dequeue(int maxBatchSize) throws IOException {
      return consumers.peek().dequeue(maxBatchSize);
    }

    @Override
    public void close() throws IOException {
      for (HBaseQueueConsumer consumer : consumers) {
        Closeables.closeQuietly(consumer);
      }
    }

    @Override
    public void startTx(Transaction tx) {
      // This is a fallback case if we failed to update the consumers in the postTxCommit call.
      if (consumers.isEmpty()) {
        updateConsumers();
      }
      super.startTx(tx);
    }

    @Override
    public void postTxCommit() {
      super.postTxCommit();
      HBaseQueueConsumer consumer = consumers.poll();
      if (!consumer.isClosed()) {
        consumers.add(consumer);
      }
      if (consumers.isEmpty()) {
        updateConsumers();
      }
    }

    private void updateConsumers() {
      try {
        Iterables.addAll(consumers, consumerCreator.call());
      } catch (Exception e) {
        LOG.error("Failed to update consumer", e);
        // Nothing much can be done except propagating
        throw Throwables.propagate(e);
      }
    }
  }
}
