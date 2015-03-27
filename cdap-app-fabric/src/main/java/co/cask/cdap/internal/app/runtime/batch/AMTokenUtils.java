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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.common.security.YarnTokenUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ClientServiceDelegate;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.impl.pb.client.MRClientProtocolPBClientImpl;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.internal.yarn.YarnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.ResourceManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for getting JobHistoryServer security delegation token.
 */
public final class AMTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(YarnTokenUtils.class);

  /**
   * Gets a JHS delegation token and stores it in the given Credentials.
   *
   * @return the same Credentials instance as the one given in parameter.
   */
  public static MRClientProtocol getMRClientProtocol(Configuration configuration, ApplicationId applicationId)
    throws IOException, YarnException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return null;
    }

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(configuration);
    yarnClient.start();
    try {
      InetSocketAddress address = YarnUtils.getRMAddress(configuration);
      LOG.info("MAPREDUCE ApplicationId is {}", applicationId);
      while (!yarnClient.getApplicationReport(applicationId).
        getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
        continue;
      }
      ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
      LOG.info("Application Report is {}", applicationReport);

      InetSocketAddress clientAddress = NetUtils.createSocketAddrForHost(applicationReport.getHost(),
                                                                         applicationReport.getRpcPort());
      org.apache.hadoop.security.token.Token<TokenIdentifier> token =
        ConverterUtils.convertFromYarn(applicationReport.getClientToAMToken(), clientAddress);

      LOG.info("!!!!MRM Adding client AM token {}", token);
      //DistributedMapReduceProgramRunner.addToSecureStore(new Text(token.getService()), token);
      LOG.info("Current USER : {}", UserGroupInformation.getCurrentUser());
      LOG.info("Current user credentials : {}",
               UserGroupInformation.getCurrentUser().getCredentials().getAllTokens());
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      credentials.addToken(new Text(applicationReport.getClientToAMToken().getService()), token);
      LOG.info("Current user credentials after adding client token : {}",
               UserGroupInformation.getCurrentUser().getCredentials().getAllTokens());
      UserGroupInformation.getCurrentUser().addCredentials(credentials);
      LOG.info("2 Current user credentials after adding client token : {}",
               UserGroupInformation.getCurrentUser().getCredentials().getAllTokens());

      ResourceMgrDelegate resourceMgrDelegate = new ResourceMgrDelegate(new YarnConfiguration(configuration));
      MRClientCache clientCache = new MRClientCache(configuration, resourceMgrDelegate);
      MRClientProtocol amProxy = clientCache.getAMProxy(clientAddress);
      LOG.info("3 Constructed MRClientProtocol for AMProxy  : {}", amProxy.getConnectAddress());
      return amProxy;
    } finally {
      yarnClient.stop();
    }
  }

  /**
   * Overridden class to expose {@code getInitializedHSProxy}.
   */
  private static class MRClientCache extends ClientCache {
    private Configuration configuration;
    private ResourceMgrDelegate rm;
    private Map<JobID, ClientServiceDelegate> cache =
      new HashMap<JobID, ClientServiceDelegate>();
    private MRClientProtocol hsProxy;

    public MRClientCache(Configuration conf, ResourceMgrDelegate rm) {
      super(conf, rm);
      this.configuration = conf;
      this.rm = rm;
    }

    public MRClientProtocol getAMProxy(final InetSocketAddress clientAddress)
      throws IOException {
      LOG.debug("Connecting to AM at: " + clientAddress);
      final YarnRPC rpc = YarnRPC.create(configuration);
      LOG.debug("Connected to AM at: " + clientAddress);
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      LOG.info("AM PROXY CurrentUser Credentials Tokens {}", currentUser.getCredentials().getAllTokens());
      return currentUser.doAs(new PrivilegedAction<MRClientProtocol>() {
        @Override
        public MRClientProtocol run() {
          return (MRClientProtocol) rpc.getProxy(MRClientProtocol.class,
                                                 clientAddress, configuration);
        }
      });
    }
  }
}

