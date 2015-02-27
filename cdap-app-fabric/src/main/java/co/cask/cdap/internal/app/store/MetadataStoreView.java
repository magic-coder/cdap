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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.Id;

/**
 * Store for application metadata
 */
public abstract class MetadataStoreView
  <ID extends Id, VALUE, NOT_FOUND extends Throwable, ALREADY_EXISTS extends Throwable> {

  protected final MetadataStoreDataset metadataStore;
  protected final Class<VALUE> valueType;
  protected final String partition;

  /**
   * Constructs a view of the {@link MetadataStoreDataset} which fetches VALUEs keyed by (partition, getKey()).
   *
   * @param metadataStore the {@link MetadataStoreDataset}
   * @param partition partition within the {@link MetadataStoreDataset} to store VALUEs
   * @param valueType type of object to store
   */
  public MetadataStoreView(MetadataStoreDataset metadataStore, String partition, Class<VALUE> valueType) {
    this.metadataStore = metadataStore;
    this.partition = partition;
    this.valueType = valueType;
  }

  protected abstract NOT_FOUND notFound(ID id);
  protected abstract ALREADY_EXISTS alreadyExists(ID id);
  protected abstract MDSKey getKey(ID id);

  protected MDSKey getRealKey(ID id) {
    return new MDSKey.Builder()
      .add(partition)
      .add(getKey(id))
      .build();
  }

  public VALUE get(ID id) throws NOT_FOUND {
    VALUE appMeta = metadataStore.get(getRealKey(id), valueType);
    if (appMeta == null) {
      throw notFound(id);
    }
    return appMeta;
  }

  public void create(ID id, VALUE value) throws ALREADY_EXISTS {
    assertNotExists(id);
    metadataStore.write(getRealKey(id), value);
  }

  public void update(ID id, VALUE value) throws NOT_FOUND {
    assertExists(id);
    metadataStore.write(getRealKey(id), value);
  }

  public void delete(ID id) throws NOT_FOUND {
    assertExists(id);
    metadataStore.deleteAll(getRealKey(id));
  }

  public boolean exists(ID id) {
    return metadataStore.exists(getRealKey(id));
  }

  public void assertExists(ID id) throws NOT_FOUND {
    if (!exists(id)) {
      throw notFound(id);
    }
  }

  public void assertNotExists(ID id) throws ALREADY_EXISTS {
    if (!exists(id)) {
      throw alreadyExists(id);
    }
  }
}
