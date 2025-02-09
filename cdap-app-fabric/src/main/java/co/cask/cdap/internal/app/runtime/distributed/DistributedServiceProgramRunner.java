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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Distributed ProgramRunner for Service.
 */
public class DistributedServiceProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedServiceProgramRunner.class);

  @Inject
  DistributedServiceProgramRunner(TwillRunner twillRunner, Configuration hConfig, CConfiguration cConfig) {
    super(twillRunner, hConfig, cConfig);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options, File hConfFile, File cConfFile,
                                     ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.SERVICE, "Only SERVICE process type is supported.");

    final ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());
    Preconditions.checkNotNull(serviceSpec, "Missing ServiceSpecification for %s", program.getName());

    // Launch service runnables program runners
    LOG.info("Launching distributed service: {}:{}", program.getName(), serviceSpec.getName());

    TwillController controller = launcher.launch(new ServiceTwillApplication(program, serviceSpec, hConfFile,
                                                                             cConfFile, eventHandler));

    DistributedServiceRunnableInstanceUpdater instanceUpdater = new DistributedServiceRunnableInstanceUpdater(
      program, controller);

    return new ServiceTwillProgramController(program.getName(), controller, instanceUpdater).startListen();
  }

  @Override
  protected EventHandler createEventHandler(CConfiguration cConf) {
    return new AbortOnTimeoutEventHandler(
      cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE), true);
  }
}
