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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Map;

/**
 * {@link co.cask.cdap.api.dataset.DatasetDefinition} for {@link CounterTimeseriesTable}.
 */
@Beta
public class CounterTimeseriesTableDefinition
  extends AbstractDatasetDefinition<CounterTimeseriesTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public CounterTimeseriesTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("ts", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("ts"), classLoader);
  }

  @Override
  public CounterTimeseriesTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                           ClassLoader classLoader, Map<String, String> arguments) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("ts"), classLoader, arguments);
    return new CounterTimeseriesTable(spec, table);
  }
}
