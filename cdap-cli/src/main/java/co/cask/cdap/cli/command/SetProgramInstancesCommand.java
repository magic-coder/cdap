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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.ProgramClient;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Sets the instances of a program.
 */
public class SetProgramInstancesCommand extends AbstractAuthCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  public SetProgramInstancesCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    String appId = programIdParts[0];
    int numInstances = arguments.getInt(ArgumentName.NUM_INSTANCES.toString());

    switch (elementType) {
      case FLOWLET:
        if (programIdParts.length < 3) {
          throw new CommandInputError(this);
        }
        String flowId = programIdParts[1];
        String flowletId = programIdParts[2];
        programClient.setFlowletInstances(appId, flowId, flowletId, numInstances);
        output.printf("Successfully set flowlet '%s' of flow '%s' of app '%s' to %d instances\n",
                      flowId, flowletId, appId, numInstances);
        break;
      case WORKER:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String workerId = programIdParts[1];
        programClient.setWorkerInstances(appId, workerId, numInstances);
        output.printf("Successfully set worker '%s' of app '%s' to %d instances\n",
                      workerId, appId, numInstances);
        break;
      case PROCEDURE:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String procedureId = programIdParts[1];
        programClient.setProcedureInstances(appId, procedureId, numInstances);
        output.printf("Successfully set procedure '%s' of app '%s' to %d instances\n",
                      procedureId, appId, numInstances);
        break;
      case SERVICE:
        if (programIdParts.length < 2) {
          throw new CommandInputError(this);
        }
        String service = programIdParts[1];
        programClient.setServiceInstances(appId, service, numInstances);
        output.printf("Successfully set service '%s' of app '%s' to %d instances\n", service, appId, numInstances);
        break;
      case RUNNABLE:
        if (programIdParts.length < 3) {
          throw new CommandInputError(this);
        }
        String serviceId = programIdParts[1];
        String runnableId = programIdParts[2];
        programClient.setServiceRunnableInstances(appId, serviceId, runnableId, numInstances);
        output.printf("Successfully set runnable '%s' of service '%s' of app '%s' to %d instances\n",
                      runnableId, serviceId, appId, numInstances);
        break;
      default:
        // TODO: remove this
        throw new IllegalArgumentException("Unrecognized program element type for scaling: " + elementType);
    }
  }

  @Override
  public String getPattern() {
    return String.format("set %s instances <%s> <%s>", elementType.getName(),
                         elementType.getArgumentName(), ArgumentName.NUM_INSTANCES);
  }

  @Override
  public String getDescription() {
    return String.format("Sets the instances of a %s.", elementType.getPrettyName());

  }
}
