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

package co.cask.cdap.cli.exception;

import co.cask.cdap.client.config.ConnectionConfig;

import javax.annotation.Nullable;

/**
 * Thrown when there is an issue in connecting to a CDAP instance.
 */
public class ConnectionFailureException extends Exception {

  public ConnectionFailureException(ConnectionConfig config, @Nullable Throwable cause) {
    super("Failed to connect to " + config.getBaseURI(), cause);
  }

}
