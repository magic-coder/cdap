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

import co.cask.cdap.common.exception.NamespaceAlreadyExistsException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.dataset2.lib.table.MetadataStoreDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;

import java.util.List;

/**
 * Store for application metadata
 */
public abstract class NamespacedMetadataStoreView
  <ID extends Id.NamespacedId, VALUE, NOT_FOUND extends Throwable, ALREADY_EXISTS extends Throwable>
  extends MetadataStoreView
  <ID, VALUE, NOT_FOUND, ALREADY_EXISTS>{

  private final MetadataStoreView<Id.Namespace, NamespaceMeta,
    NamespaceNotFoundException, NamespaceAlreadyExistsException> namespaceView;

  public NamespacedMetadataStoreView(MetadataStoreDataset metadataStore, String partition, Class<VALUE> valueType,
                                     MetadataStoreView<Id.Namespace, NamespaceMeta,
                                       NamespaceNotFoundException, NamespaceAlreadyExistsException> namespaceView) {
    super(metadataStore, partition, valueType);
    this.namespaceView = namespaceView;
  }

  protected abstract NOT_FOUND notFound(ID id);
  protected abstract ALREADY_EXISTS alreadyExists(ID id);
  protected abstract MDSKey getChildKey(ID id);

  @Override
  protected MDSKey getKey(ID id) {
    MDSKey childKey = getChildKey(id);
    return new MDSKey.Builder()
      .add(partition)
      .add(id.getNamespace().getId())
      .add(childKey.getKey())
      .build();
  }

  private MDSKey getListKey(Id.Namespace namespace) {
    return new MDSKey.Builder()
      .add(partition)
      .add(namespace.getId())
      .build();
  }

  public List<VALUE> list(Id.Namespace namespace) {
    return metadataStore.list(getListKey(namespace), valueType);
  }

  public void deleteAll(Id.Namespace namespace) {
    metadataStore.deleteAll(getListKey(namespace));
  }
}
