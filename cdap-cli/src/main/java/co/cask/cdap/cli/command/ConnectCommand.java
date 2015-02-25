/*
 * Copyright © 2012-2015 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ConnectionManager;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;

import java.io.PrintStream;
import java.net.URI;
import javax.inject.Inject;

/**
 * Connects to a CDAP instance.
 */
public class ConnectCommand implements Command {

  private final CLIConfig cliConfig;
  private final CConfiguration cConf;
  private final ConnectionManager connectionManager;

  @Inject
  public ConnectCommand(CLIConfig cliConfig, CConfiguration cConf, ConnectionManager connectionManager) {
    this.cConf = cConf;
    this.cliConfig = cliConfig;
    this.connectionManager = connectionManager;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String uriString = arguments.get("cdap-instance-uri");
    if (!uriString.contains("://")) {
      uriString = "http://" + uriString;
    }

    URI uri = URI.create(uriString);
    String hostname = uri.getHost();
    boolean sslEnabled = "https".equals(uri.getScheme());
    int port = uri.getPort();

    if (port == -1) {
      port = sslEnabled ?
        cConf.getInt(Constants.Router.ROUTER_SSL_PORT) :
        cConf.getInt(Constants.Router.ROUTER_PORT);
    }

    try {
      // TODO: make user and namespace configurable via this command
      ConnectionConfig connectionConfig = new ConnectionConfig(null, sslEnabled, hostname, port,
                                                               Constants.DEFAULT_NAMESPACE,
                                                               null, cliConfig.getClientConfig().getApiVersion());
      connectionManager.tryConnect(connectionConfig);
    } catch (Exception e) {
      output.println("Failed to connect to " + uriString + ": " + e.getMessage());
    }
  }

  @Override
  public String getPattern() {
    return "connect <cdap-instance-uri>";
  }

  @Override
  public String getDescription() {
    return "Connects to a CDAP instance.";
  }
}
