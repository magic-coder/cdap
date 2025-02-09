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
package co.cask.cdap.common.http;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.HttpExceptionHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Function;
import com.google.inject.Inject;
import org.jboss.netty.channel.ChannelPipeline;

/**
 * Provides a {@link NettyHttpService.Builder} that has common settings built-in.
 */
public class CommonNettyHttpServiceBuilder extends NettyHttpService.Builder {
  public CommonNettyHttpServiceBuilder(CConfiguration configuration) {
    super();
    if (configuration.getBoolean(Constants.Security.AUTHORIZATION_ENABLED)) {
      this.modifyChannelPipeline(new Function<ChannelPipeline, ChannelPipeline>() {
        @Override
        public ChannelPipeline apply(ChannelPipeline input) {
          input.addAfter("decoder", "authenticator", new AuthenticationChannelHandler());
          return input;
        }
      });
    }
    this.setExceptionHandler(new HttpExceptionHandler());
  }
}
