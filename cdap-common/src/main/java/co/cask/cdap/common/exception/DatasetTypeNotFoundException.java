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
 * Thrown when a dataset type is not found
 */
public class DatasetTypeNotFoundException extends NotFoundException {

  private final String datasetType;

  public DatasetTypeNotFoundException(String datasetType) {
    super("dataset type", datasetType);
    this.datasetType = datasetType;
  }

  /**
   * @return the dataset type that was not found
   */
  public String getDatasetType() {
    return datasetType;
  }
}
