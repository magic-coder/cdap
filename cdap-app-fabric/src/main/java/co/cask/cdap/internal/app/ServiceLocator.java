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

package co.cask.cdap.internal.app;

import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Locates services.
 */
public final class ServiceLocator {

  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, Supplier<Discoverable>> suppliers;

  public ServiceLocator(DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.suppliers = Maps.newHashMap();
  }

  public URI locate(final String discoverableName) throws IOException {
    if (!suppliers.containsKey(discoverableName)) {
      suppliers.put(discoverableName, new Supplier<Discoverable>() {
        @Override
        public Discoverable get() {
          ServiceDiscovered discovered = discoveryServiceClient.discover(discoverableName);
          return new RandomEndpointStrategy(discovered).pick(5, TimeUnit.SECONDS);
        }
      });
    }

    Discoverable discoverable = suppliers.get(discoverableName).get();
    if (discoverable == null) {
      throw new IOException("Can't find " + discoverableName + " endpoint");
    }

    return URI.create(String.format(
      "http://%s:%d",
      discoverable.getSocketAddress().getHostName(),
      discoverable.getSocketAddress().getPort()));
  }

}
