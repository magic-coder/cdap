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
 * the License
 */

package co.cask.cdap.cli.command;

import co.cask.cdap.api.service.Service;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.StringUtils;
import co.cask.common.cli.Arguments;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Call an endpoint of a {@link Service}.
 */
public class CallServiceCommand extends AbstractCommand implements Categorized {
  private static final Gson GSON = new Gson();


  private final ClientConfig clientConfig;
  private final RESTClient restClient;
  private final ServiceClient serviceClient;
  private final FilePathResolver filePathResolver;

  @Inject
  public CallServiceCommand(ClientConfig clientConfig, RESTClient restClient,
                            ServiceClient serviceClient, CLIConfig cliConfig,
                            FilePathResolver filePathResolver) {
    super(cliConfig);
    this.clientConfig = clientConfig;
    this.restClient = restClient;
    this.serviceClient = serviceClient;
    this.filePathResolver = filePathResolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] appAndServiceId = arguments.get(ArgumentName.SERVICE.toString()).split("\\.");
    if (appAndServiceId.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = appAndServiceId[0];
    String serviceId = appAndServiceId[1];
    String method = arguments.get(ArgumentName.HTTP_METHOD.toString());
    String path = arguments.get(ArgumentName.ENDPOINT.toString());
    path = path.startsWith("/") ? path.substring(1) : path;
    String headers = arguments.get(ArgumentName.HEADERS.toString(), "");
    String bodyString = arguments.get(ArgumentName.HTTP_BODY.toString(), "");
    String bodyFile = arguments.get(ArgumentName.LOCAL_FILE_PATH.toString(), "");
    if (!bodyString.isEmpty() && !bodyFile.isEmpty()) {
      String message = String.format("Please provide either [body <%s>] or [body:file <%s>], " +
                                       "but not both", ArgumentName.HTTP_BODY.toString(),
                                     ArgumentName.LOCAL_FILE_PATH.toString());
      throw new CommandInputError(this, message);
    }

    Map<String, String> headerMap = GSON.fromJson(headers, new TypeToken<Map<String, String>>() {
    }.getType());
    URL url = new URL(serviceClient.getServiceURL(appId, serviceId), path);

    HttpMethod httpMethod = HttpMethod.valueOf(method);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url).addHeaders(headerMap);
    if (httpMethod == HttpMethod.GET && (!bodyFile.isEmpty() || !bodyString.isEmpty())) {
      throw new UnsupportedOperationException("Sending body in a GET request is not supported");
    }

    if (!bodyFile.isEmpty()) {
      builder.withBody(filePathResolver.resolvePathToFile(bodyFile));
    } else if (!bodyString.isEmpty()) {
      builder.withBody(bodyString);
    }

    HttpResponse response = restClient.execute(builder.build(), clientConfig.getAccessToken());

    Table table = Table.builder()
      .setHeader("status", "headers", "body size", "body")
      .setRows(ImmutableList.of(response), new RowMaker<HttpResponse>() {
        @Override
        public List<?> makeRow(HttpResponse httpResponse) {
          ByteBuffer byteBuffer = ByteBuffer.wrap(httpResponse.getResponseBody());
          long bodySize = byteBuffer.remaining();
          return Lists.newArrayList(httpResponse.getResponseCode(), formatHeaders(httpResponse),
                                    bodySize, getBody(byteBuffer));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("call service <%s> <%s> <%s> [headers <%s>] [body <%s>] [body:file <%s>]",
                         ArgumentName.SERVICE, ArgumentName.HTTP_METHOD,
                         ArgumentName.ENDPOINT, ArgumentName.HEADERS, ArgumentName.HTTP_BODY,
                         ArgumentName.LOCAL_FILE_PATH);
  }

  @Override
  public String getDescription() {
    return String.format("Calls a %s endpoint. The <%s> are formatted as \"{'key':'value', ...}\"." +
                         " The request body may be provided either as a string or a file." +
                         " To provide the body as a string, use \"body <%s>\"." +
                         " To provide the body as a file, use \"body:file <%s>\".",
                         ElementType.SERVICE.getPrettyName(),
                         ArgumentName.HEADERS, ArgumentName.HTTP_BODY, ArgumentName.LOCAL_FILE_PATH);
  }

  /**
   * Format multiple header values as a comma separated list of the values.
   * This is a valid formatting: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html
   */
  private String formatHeaders(HttpResponse response) {
    Multimap<String, String> headers = response.getHeaders();
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
    for (String key : headers.keySet()) {
      Collection<String> value = headers.get(key);
      builder.put(key, StringUtils.arrayToString(value.toArray(new String[value.size()])));
    }
    return formatHeader(builder.build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.EGRESS.getName();
  }
}
