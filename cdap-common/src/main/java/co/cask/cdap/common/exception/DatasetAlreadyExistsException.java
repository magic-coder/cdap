/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.exception;

/**
 * Thrown when the user tries to create a dataset, but a dataset already exists by that name.
 */
public class DatasetAlreadyExistsException extends AlreadyExistsException {

  private final String datasetName;

  public DatasetAlreadyExistsException(String datasetName) {
    super("dataset", datasetName);
    this.datasetName = datasetName;
  }

  /**
   * @return Name of the existing dataset
   */
  public String getDatasetName() {
    return datasetName;
  }
}
