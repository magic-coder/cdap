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

package co.cask.cdap.client.util;

import co.cask.cdap.security.authentication.client.AccessToken;

/**
 *
 */
public class UserAccessToken extends AccessToken {

  private final String user;

  /**
   * Constructs new instance of the access token.
   *
   * @param user      name of the user that this access token belongs to
   * @param value     string value of the access token
   * @param expiresIn token validity lifetime in seconds
   * @param tokenType access token type supported by the authentication server.
   */
  public UserAccessToken(String user, String value, Long expiresIn, String tokenType) {
    super(value, expiresIn, tokenType);
    this.user = user;
  }

  public String getUser() {
    return user;
  }
}
