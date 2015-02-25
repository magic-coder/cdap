/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.util;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ConnectionManager;
import co.cask.cdap.cli.LoginFlow;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.UserAccessToken;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;

import java.io.PrintStream;

/**
 * Abstract command for updating {@link co.cask.cdap.security.authentication.client.AccessToken}.
 */
public abstract class AbstractAuthCommand implements Command {

  private final CLIConfig cliConfig;
  private final LoginFlow loginFlow;
  private final ConnectionManager connectionManager;

  public AbstractAuthCommand(CLIConfig cliConfig, ConnectionManager connectionManager, LoginFlow loginFlow) {
    this.cliConfig = cliConfig;
    this.connectionManager = connectionManager;
    this.loginFlow = loginFlow;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    try {
      perform(arguments, output);
    } catch (UnAuthorizedAccessTokenException e) {
      ConnectionConfig originalConfig = cliConfig.getClientConfig().getConnectionConfig();
      UserAccessToken token = loginFlow.login(originalConfig);
      ConnectionConfig newConfig = new ConnectionConfig.Builder(originalConfig)
        .setAccessToken(token)
        .setUser(token.getUser())
        .build();
      if (connectionManager.isConnectionValid(cliConfig.getClientConfig(), newConfig)) {
        cliConfig.setConnectionConfig(newConfig);
      }
      cliConfig.getClientConfig().getConnectionConfig().setAccessToken(token);
      cliConfig.getClientConfig().getConnectionConfig().setUser(token.getUser());
      perform(arguments, output);
    }
  }

  public abstract void perform(Arguments arguments, PrintStream output) throws Exception;
}
