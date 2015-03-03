/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.NamespaceAlreadyExistsException;
import co.cask.cdap.common.exception.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.namespace.NamespaceCannotBeDeletedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.CharMatcher;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link HttpHandler} for handling REST calls to namespace endpoints.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class NamespaceHttpHandler extends AbstractAppFabricHttpHandler {

  private final NamespaceAdmin namespaceAdmin;

  @Inject
  public NamespaceHttpHandler(Authenticator authenticator, NamespaceAdmin namespaceAdmin) {
    super(authenticator);
    this.namespaceAdmin = namespaceAdmin;
  }

  @GET
  @Path("/namespaces")
  public void getAllNamespaces(HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, namespaceAdmin.listNamespaces());
  }

  @GET
  @Path("/namespaces/{namespace-id}")
  public void getNamespace(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId) throws NamespaceNotFoundException {
    NamespaceMeta ns = namespaceAdmin.getNamespace(Id.Namespace.from(namespaceId));
    responder.sendJson(HttpResponseStatus.OK, ns);
  }

  @PUT
  @Path("/namespaces/{namespace-id}")
  public void create(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId)
    throws BadRequestException, IOException, NamespaceCannotBeCreatedException, NamespaceAlreadyExistsException {

    NamespaceMeta metadata = parseBody(request, NamespaceMeta.class);

    checkValid(namespaceId);
    checkNotReserved(namespaceId);

    NamespaceMeta.Builder namespaceMeta = new NamespaceMeta.Builder().setId(namespaceId);
    if (metadata != null && metadata.getName() != null) {
      namespaceMeta.setName(metadata.getName());
    }
    if (metadata != null && metadata.getDescription() != null) {
      namespaceMeta.setDescription(metadata.getDescription());
    }

    namespaceAdmin.createNamespace(namespaceMeta.build());
    responder.sendString(HttpResponseStatus.OK, String.format("Namespace '%s' created successfully.", namespaceId));
  }

  @DELETE
  @Path("/namespaces/{namespace-id}")
  public void delete(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespace)
    throws BadRequestException, NamespaceNotFoundException, NamespaceCannotBeDeletedException {

    checkNotReserved(namespace);
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    namespaceAdmin.deleteNamespace(namespaceId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void checkValid(String namespaceId) throws BadRequestException {
    // TODO: This is copied from StreamVerification in app-fabric as this handler is in data-fabric module.
    boolean valid = CharMatcher.inRange('A', 'Z')
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.is('-'))
      .or(CharMatcher.is('_'))
      .or(CharMatcher.inRange('0', '9')).matchesAllOf(namespaceId);
    if (!valid) {
      throw new BadRequestException("Namespace id can contain only alphanumeric characters, '-' or '_'.");
    }
  }

  private void checkNotReserved(String namespaceId) throws BadRequestException {
    boolean reserved = Constants.DEFAULT_NAMESPACE.equals(namespaceId) ||
      Constants.SYSTEM_NAMESPACE.equals(namespaceId) ||
      Constants.Logging.SYSTEM_NAME.equals(namespaceId);
    if (reserved) {
      throw new BadRequestException(String.format("Cannot delete the namespace '%s'. '%s' is a reserved namespace.",
                                                  namespaceId, namespaceId));
    }
  }
}
