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

package co.cask.cdap.cli.authentication;

import com.google.common.base.Preconditions;

/**
 */
public abstract class Credentials {

  /**
   * @param string in the format "<scheme>:<credentials>"
   * @return the {@link Credentials}.
   */
  public static Credentials fromString(String string) {
    if (string == null || string.isEmpty()) {
      return null;
    }

    String[] tokens = string.split(":");
    if (tokens.length <= 1) {
      throw new IllegalArgumentException("Invalid credentials format: " + string);
    }

    String scheme = tokens[0];
    String credentials = tokens[1];

    if (Token.SCHEME.equals(scheme)) {
      return Token.fromString(credentials);
    } else if (Username.SCHEME.equals(scheme)) {
      return Username.fromString(credentials);
    }

    throw new IllegalArgumentException("Invalid scheme: " + scheme);
  }

  /**
   *
   */
  public static final class Token extends Credentials {

    private static final String SCHEME = "token";

    private final String tokenPath;

    public Token(String tokenPath) {
      Preconditions.checkNotNull(tokenPath);
      this.tokenPath = tokenPath;
    }

    public String getTokenPath() {
      return tokenPath;
    }

    /**
     * @param string path to the token file
     * @return the token credentials
     */
    public static Token fromString(String string) {
      return new Token(string);
    }
  }

  /**
   *
   */
  public static final class Username extends Credentials {

    private static final String SCHEME = "username";

    private final String username;
    private final String password;

    public Username(String username, String password) {
      Preconditions.checkNotNull(username);
      Preconditions.checkNotNull(password);
      this.username = username;
      this.password = password;
    }

    /**
     * @param string in the format "<username>:<password>"
     * @return the username credentials
     */
    public static Username fromString(String string) {
      String[] tokens = string.split(":");
      if (tokens.length != 1 && tokens.length != 2) {
        throw new IllegalArgumentException("Invalid username credentials format: " + string);
      }
      return new Username(tokens[0], tokens.length == 1 ? "" : tokens[1]);
    }

    public String getPassword() {
      return password;
    }

    public String getUsername() {
      return username;
    }
  }

}
