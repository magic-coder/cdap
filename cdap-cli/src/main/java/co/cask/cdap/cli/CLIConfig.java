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

package co.cask.cdap.cli;

import co.cask.cdap.cli.command.VersionCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Configuration for the CDAP CLI.
 */
public class CLIConfig {

  private final ClientConfig clientConfig;
  private final FilePathResolver resolver;
  private final String version;

  private List<ConnectionChangeListener> connectionChangeListeners;

  public CLIConfig(ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    this.resolver = new FilePathResolver();
    this.version = tryGetVersion();
    this.connectionChangeListeners = Lists.newArrayList();
  }

  public String getCurrentNamespace() {
    return clientConfig.getNamespace();
  }

  public void setCurrentNamespace(String currentNamespace) {
    clientConfig.setNamespace(currentNamespace);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  public void setConnectionConfig(ConnectionConfig config) {
    clientConfig.setConnectionConfig(config);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  private String tryGetVersion() {
    try {
      InputSupplier<? extends InputStream> versionFileSupplier = new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return VersionCommand.class.getClassLoader().getResourceAsStream("VERSION");
        }
      };
      return CharStreams.toString(CharStreams.newReaderSupplier(versionFileSupplier, Charsets.UTF_8));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public String getVersion() {
    return version;
  }

  public FilePathResolver getResolver() {
    return resolver;
  }

  public void setConnection(ConnectionConfig connection) {
    clientConfig.setConnectionConfig(connection);
    for (ConnectionChangeListener listener : connectionChangeListeners) {
      listener.onConnectionChanged(clientConfig);
    }
  }

  public void addHostnameChangeListener(ConnectionChangeListener listener) {
    this.connectionChangeListeners.add(listener);
  }

  /**
   * Listener for hostname changes.
   */
  public interface ConnectionChangeListener {
    void onConnectionChanged(ClientConfig clientConfig);
  }
}
