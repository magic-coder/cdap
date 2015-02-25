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
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.UserAccessToken;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;
import co.cask.cdap.security.authentication.client.Credential;
import co.cask.cdap.security.authentication.client.basic.BasicAuthenticationClient;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import jline.console.ConsoleReader;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Provides a login flow to the user.
 */
public class LoginFlow {

  private final PrintStream output;
  private final boolean verbose;
  private final boolean verifySSLCert;
  private final ConnectionManager connectionManager;

  public LoginFlow(ConnectionManager connectionManager, PrintStream output, boolean verifySSLCert, boolean verbose) {
    this.connectionManager = connectionManager;
    this.output = output;
    this.verifySSLCert = verifySSLCert;
    this.verbose = verbose;
  }


  public UserAccessToken transform(ConnectionConfig config, Credentials credentials) throws IOException {
    if (credentials instanceof Credentials.Token) {
      Credentials.Token token = (Credentials.Token) credentials;
      FilePathResolver resolver = new FilePathResolver();
      String tokenValue = Files.readFirstLine(resolver.resolvePathToFile(token.getTokenPath()), Charsets.UTF_8);
      return new UserAccessToken(null, tokenValue, -1L, null);
    } else if (credentials instanceof Credentials.Username) {
      Credentials.Username username = (Credentials.Username) credentials;
      return login(config, username.getUsername(), username.getPassword());
    }
    throw new IllegalArgumentException("Invalid Credentials type: " + credentials.getClass().getName());
  }

  public UserAccessToken login(ConnectionConfig config) throws IOException {
    return login(config, null, null);
  }

  public UserAccessToken login(ConnectionConfig config,
                               @Nullable String username, @Nullable String password) throws IOException {
    AuthenticationClient authenticationClient = new BasicAuthenticationClient();
    authenticationClient.setConnectionInfo(config.getHostname(), config.getPort(), config.isSSLEnabled());

    Properties properties = new Properties();
    properties.put(BasicAuthenticationClient.VERIFY_SSL_CERT_PROP_NAME, String.valueOf(verifySSLCert));

    // obtain new access token via manual user input
    output.printf("Authentication is enabled in the CDAP instance: %s.\n", config.getHostname());
    ConsoleReader reader = new ConsoleReader();
    for (Credential credential : authenticationClient.getRequiredCredentials()) {
      String prompt = "Please, specify " + credential.getDescription() + "> ";
      String credentialValue;

      if (credential.getName().contains("username") && username != null) {
        credentialValue = username;
        output.println(username);
      } else if (credential.getName().contains("password") && password != null) {
        credentialValue = password;
        output.println(Strings.repeat("*", password.length()));
      } else {
        if (credential.isSecret()) {
          credentialValue = reader.readLine(prompt, '*');
        } else {
          credentialValue = reader.readLine(prompt);
        }
      }

      properties.put(credential.getName(), credentialValue);
      if (credential.getName().contains("username") && username == null) {
        username = credentialValue;
      } else if (credential.getName().contains("password") && password == null) {
        password = credentialValue;
      }
    }

    authenticationClient.configure(properties);
    AccessToken token = authenticationClient.getAccessToken();
    if (token != null) {
      File tokenFile = connectionManager.getTokenFile(config.getHostname(), username);
      try {
        connectionManager.cacheToken(config.getHostname(), username, token);
        if (verbose) {
          output.printf("Cached access token to %s", tokenFile.getAbsolutePath());
        }
      } catch (IOException e) {
        if (verbose) {
          output.printf("Failed to cache access token to %s", tokenFile.getAbsolutePath());
          e.printStackTrace(output);
        }
      }
      output.println();
    }

    if (token != null) {
      return new UserAccessToken(username, token.getValue(), token.getExpiresIn(), token.getTokenType());
    } else {
      return null;
    }
  }

}
