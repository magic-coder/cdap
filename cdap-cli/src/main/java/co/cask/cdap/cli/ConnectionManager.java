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

package co.cask.cdap.cli;

import co.cask.cdap.cli.authentication.Credentials;
import co.cask.cdap.cli.exception.ConnectionFailureException;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.UserAccessToken;
import co.cask.cdap.common.exception.UnAuthorizedAccessTokenException;
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import javax.annotation.Nullable;

/**
 * Manages the connection to a CDAP instance.
 */
public class ConnectionManager {

  private final ClientConfig clientConfig;
  private final FilePathResolver resolver;
  private final boolean verbose;

  @Inject
  public ConnectionManager(ClientConfig clientConfig, FilePathResolver resolver,
                           @Named(CLIMain.NAME_VERBOSE) boolean verbose) {
    this.clientConfig = clientConfig;
    this.resolver = resolver;
    this.verbose = verbose;
  }

  /**
   * @param config connection configuration to test
   * @return the config that works
   * @throws ConnectionFailureException if unable to connect using the provided config
   * @throws UnAuthorizedAccessTokenException if connection worked, but received 401 Unauthorized from server
   */
  public ConnectionConfig tryConnect(ConnectionConfig config)
    throws ConnectionFailureException, UnAuthorizedAccessTokenException {

    try {
      ConnectionConfig result = new ConnectionConfig.Builder(config).build();
      try {
        // 1. Try provided config. If succeeds, return provided config
        if (isConnectionValid(clientConfig, result)) {
          return config;
        }
      } catch (UnAuthorizedAccessTokenException e) {
        // 2. If provided config fails due to 401 Unauthorized,
        AccessToken cachedAccessToken = getCachedToken(config.getUser(), config.getHostname());
        // 2.1. If cached access token exists, try it at ~/.cdap.<hostname>.<user>.accesstoken
        if (cachedAccessToken != null) {
          result.setAccessToken(cachedAccessToken);
          if (isConnectionValid(clientConfig, result)) {
            return result;
          }
        }
        throw e;
      }
      throw new ConnectionFailureException(config, null);
    } catch (Exception e) {
      // 3. If provided config fails due to any other error, throw ConnectionFailureException
      throw new ConnectionFailureException(config, e);
    }
  }

  public boolean isConnectionValid(ClientConfig clientConfig,
                                    ConnectionConfig config) throws UnAuthorizedAccessTokenException {
    try {
      ClientConfig test = new ClientConfig.Builder(clientConfig).setConnectionConfig(config).build();
      MetaClient metaClient = new MetaClient(test);
      metaClient.ping();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public AccessToken getCachedToken(String hostname, @Nullable String username) throws IOException {
    File file = getTokenFile(hostname, username);
    String firstLine = Files.readFirstLine(file, Charsets.UTF_8);
    return new AccessToken(firstLine, -1L, null);
  }

  public void cacheToken(String hostname, @Nullable String username, AccessToken token) throws IOException {
    File file = getTokenFile(hostname, username);
    Files.write(token.getValue(), file, Charsets.UTF_8);
  }

  public File getTokenFile(String hostname, @Nullable String username) {
    String path = String.format("~/.cdap.%s.%s.accesstoken", hostname, Optional.fromNullable(username).or(""));
    return resolver.resolvePathToFile(path);
  }
}
