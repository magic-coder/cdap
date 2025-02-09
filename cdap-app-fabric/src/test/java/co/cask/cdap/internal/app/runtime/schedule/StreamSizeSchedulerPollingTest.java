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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class StreamSizeSchedulerPollingTest extends SchedulerTestBase {

  @BeforeClass
  public static void init() throws Exception {
    CCONF.setLong(Constants.Notification.Stream.STREAM_SIZE_SCHEDULE_POLLING_DELAY, 1);
    SchedulerTestBase.init();
  }

  @Override
  protected StreamMetricsPublisher createMetricsPublisher(final Id.Stream streamId) {
    return new StreamMetricsPublisher() {
      @Override
      public void increment(long size) throws Exception {
        metricStore.add(new MetricValue(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, streamId.getNamespaceId(),
                                                        Constants.Metrics.Tag.STREAM, streamId.getName()),
                                        "collect.bytes", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                        size, MetricType.COUNTER));
      }
    };
  }
}
