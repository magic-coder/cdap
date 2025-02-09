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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.AppWithDataset;
import co.cask.cdap.AppWithDatasetDuplicate;
import co.cask.cdap.AppWithSchedule;
import co.cask.cdap.AppWithServices;
import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.DummyAppWithTrackingTable;
import co.cask.cdap.MultiStreamApp;
import co.cask.cdap.SleepingWorkflowApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.WorkflowApp;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.gateway.handlers.AppFabricHttpHandler;
import co.cask.cdap.internal.app.ServiceSpecificationCodec;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.HttpServiceSpecificationCodec;
import co.cask.cdap.proto.codec.ScheduleSpecificationCodec;
import co.cask.cdap.proto.codec.WorkflowActionSpecificationCodec;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.XSlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.persist.TransactionSnapshot;
import co.cask.tephra.snapshot.SnapshotCodec;
import co.cask.tephra.snapshot.SnapshotCodecProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for {@link AppFabricHttpHandler}.
 */
public class AppFabricHttpHandlerTest extends AppFabricTestBase {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ScheduleSpecification.class, new ScheduleSpecificationCodec())
    .create();

  @Override
  protected String getAPIVersion() {
    return Constants.Gateway.API_VERSION_2_TOKEN;
  }

  /**
   * Creates an {@link Id.Program} in the default namespace.
   */
  private Id.Program createProgramId(ProgramType type, String appId, String id) {
    return Id.Program.from(Id.Application.from(Constants.DEFAULT_NAMESPACE, appId), type, id);
  }

  private String getRunnableStatus(Id.Program programId) throws Exception {
    HttpResponse response = doGet(String.format("/v2/apps/%s/%s/%s/status",
                                                programId.getApplicationId(),
                                                programId.getType().getCategoryName(), programId.getId()));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = GSON.fromJson(s, new TypeToken<Map<String, String>>() {
    }.getType());
    return o.get("status");
  }

  private int getFlowletInstances(Id.Program flowId, String flowletId) throws Exception {
    HttpResponse response =
      doGet(String.format("/v2/apps/%s/flows/%s/flowlets/%s/instances",
                          flowId.getApplicationId(), flowId.getId(), flowletId));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String result = EntityUtils.toString(response.getEntity());
    Map<String, String> reply = new Gson().fromJson(result, new TypeToken<Map<String, String>>() {
    }.getType());
    return Integer.parseInt(reply.get("instances"));
  }

  private void setFlowletInstances(Id.Program flowId, String flowletId, int instances) throws Exception {
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    HttpResponse response = doPut(String.format("/v2/apps/%s/flows/%s/flowlets/%s/instances",
                                                flowId.getApplicationId(), flowId.getId(), flowletId),
                                  json.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void testHistory(Class<?> app, Id.Program programId) throws Exception {
    try {
      deploy(app);
      // first run
      startProgram(programId);
      waitState(programId, "RUNNING");
      verifyHistories(programId, 1, "running");

      stopProgram(programId);
      waitState(programId, "STOPPED");
      verifyHistories(programId, 1, "killed");

      // second run
      startProgram(programId);
      waitState(programId, "RUNNING");
      verifyHistories(programId, 1, "running");

      stopProgram(programId);
      waitState(programId, "STOPPED");
      verifyHistories(programId, 2, "killed");

    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps/" + programId.getApplicationId()).getStatusLine().getStatusCode());
    }
  }

  private void verifyHistories(Id.Program programId, int size, @Nullable final String status) throws Exception {
    String path = String.format("/v2/apps/%s/%s/%s/runs",
                                       programId.getApplicationId(),
                                       programId.getType().getCategoryName(), programId.getId());
    if (status != null) {
      path += "?status=" + status;
    }
    final String historyPath = path;
    Tasks.waitFor(size, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        HttpResponse response = doGet(historyPath);
        List<Map<String, String>> result = GSON.fromJson(EntityUtils.toString(response.getEntity()),
                                                         new TypeToken<List<Map<String, String>>>() { }.getType());
        Assert.assertNotNull(result);
        return result.size();
      }
    }, 5, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }

  private void testRuntimeArgs(Class<?> app, Id.Program programId) throws Exception {
    deploy(app);

    Map<String, String> args = Maps.newHashMap();
    args.put("Key1", "Val1");
    args.put("Key2", "Val1");
    args.put("Key2", "Val1");

    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    String path = String.format("/v2/apps/%s/%s/%s/runtimeargs",
                                programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());

    response = doPut(path, argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet(path);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, String> argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertNotNull(argsRead);
    Assert.assertEquals(args.size(), argsRead.size());

    for (Map.Entry<String, String> entry : args.entrySet()) {
      Assert.assertEquals(entry.getValue(), argsRead.get(entry.getKey()));
    }

    //test empty runtime args
    response = doPut(path, "");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(path);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertEquals(0, argsRead == null ? -1 : argsRead.size());

    //test null runtime args
    response = doPut(path, null);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet(path);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    argsRead = GSON.fromJson(EntityUtils.toString(response.getEntity()),
        new TypeToken<Map<String, String>>() { }.getType());
    Assert.assertNotNull(argsRead);
    Assert.assertEquals(0, argsRead.size());
  }

  /**
   * Ping test
   */
  @Test
  public void pingTest() throws Exception {
    HttpResponse response = doGet("/v2/ping");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests history of a flow.
   */
  @Category(SlowTests.class)
  @Test
  public void testFlowHistory() throws Exception {
    testHistory(WordCountApp.class, createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow"));
  }

  /**
   * Tests history of a procedure.
   */
  @Test
  public void testProcedureHistory() throws Exception {
    testHistory(WordCountApp.class, createProgramId(ProgramType.PROCEDURE, "WordCountApp", "WordFrequency"));
  }

  /**
   * Tests history of a mapreduce.
   */
  @Category(XSlowTests.class)
  @Test
  public void testMapreduceHistory() throws Exception {
    testHistory(DummyAppWithTrackingTable.class, createProgramId(ProgramType.MAPREDUCE, "dummy", "dummy-batch"));
  }

  /**
   * Tests history of a workflow.
   */
  @Category(XSlowTests.class)
  @Test
  public void testWorkflowHistory() throws Exception {
    try {
      deploy(SleepingWorkflowApp.class);
      Id.Program workflowId = createProgramId(ProgramType.WORKFLOW, "SleepWorkflowApp", "SleepWorkflow");
      // first run with long sleep so that we can kill it
      startProgram(workflowId, ImmutableMap.of("sleep.ms", "10000"));
      waitState(workflowId, "RUNNING");
      // Kill workflow
      stopProgram(workflowId);
      // workflow stops by itself after actions are done
      waitState(workflowId, "STOPPED");

      // second run
      startProgram(workflowId, ImmutableMap.of("sleep.ms", "0"));
      // workflow stops by itself after actions are done
      waitState(workflowId, "STOPPED");

      verifyHistories(workflowId, 1, "completed");
      verifyHistories(workflowId, 1, "killed");

    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps/SleepWorkflowApp").getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testGetSetFlowletInstances() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");

    deploy(WordCountApp.class);
    Assert.assertEquals("STOPPED", getRunnableStatus(flowId));
    startProgram(flowId);
    waitState(flowId, "RUNNING");

    // Get instances for a non-existent flowlet, flow, and app.
    HttpResponse response = doGet("/v2/apps/WordCountApp/flows/WordCountFlow/flowlets/XXXX/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/WordCountApp/flows/XXXX/flowlets/StreamSource/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/XXXX/flows/WordCountFlow/flowlets/StreamSource/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());


    // PUT instances for non-existent flowlet, flow, and app.
    String payload = "{instances: 1}";
    response = doPut("/v2/apps/WordCountApp/flows/WordCountFlow/flowlets/XXXX/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doPut("/v2/apps/WordCountApp/flows/XXXX/flowlets/StreamSource/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doPut("/v2/apps/XXXX/flows/WordCountFlow/flowlets/StreamSource/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    //Get Flowlet Instances
    Assert.assertEquals(1, getFlowletInstances(flowId, "StreamSource"));

    //Set Flowlet Instances
    setFlowletInstances(flowId, "StreamSource", 3);
    Assert.assertEquals(3, getFlowletInstances(flowId, "StreamSource"));

    // Stop the flow and check its status
    stopProgram(flowId);
    waitState(flowId, "STOPPED");
  }

  @Test
  public void testChangeFlowletStreamInput() throws Exception {
    deploy(MultiStreamApp.class);

    // non-existing stream
    Assert.assertEquals(404,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream1", "notfound"));

    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream1", "stream2"));
    // stream1 is no longer a connection
    Assert.assertEquals(500,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream1", "stream3"));
    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter1", "stream2", "stream3"));

    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter2", "stream3", "stream4"));
    // stream1 is no longer a connection
    Assert.assertEquals(500,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter2", "stream3", "stream1"));
    Assert.assertEquals(200,
                        changeFlowletStreamInput("MultiStreamApp", "CounterFlow", "counter2", "stream4", "stream1"));
  }

  private int changeFlowletStreamInput(String app, String flow, String flowlet,
                                                String oldStream, String newStream) throws Exception {
    return doPut(
       String.format("/v2/apps/%s/flows/%s/flowlets/%s/connections/%s", app, flow, flowlet, newStream),
       String.format("{\"oldStreamId\":\"%s\"}", oldStream)).getStatusLine().getStatusCode();
  }


  @Category(XSlowTests.class)
  @Test
  public void testStartStop() throws Exception {
    //deploy, check the status and start a flow. Also check the status
    deploy(WordCountApp.class);

    Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");
    Assert.assertEquals("STOPPED", getRunnableStatus(flowId));
    startProgram(flowId);
    waitState(flowId, "RUNNING");

    //web-app, start, stop and status check.
    Assert.assertEquals(200,
      doPost("/v2/apps/WordCountApp/webapp/start", null).getStatusLine().getStatusCode());

    Assert.assertEquals("RUNNING", getWebappStatus("WordCountApp"));
    Assert.assertEquals(200, doPost("/v2/apps/WordCountApp/webapp/stop", null).getStatusLine().getStatusCode());
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals("STOPPED", getWebappStatus("WordCountApp"));

    // Stop the flow and check its status
    stopProgram(flowId);
    waitState(flowId, "STOPPED");

    // Check the start/stop endpoints for procedures
    Id.Program procedureId = createProgramId(ProgramType.PROCEDURE, "WordCountApp", "WordFrequency");
    Assert.assertEquals("STOPPED", getRunnableStatus(procedureId));
    startProgram(procedureId);
    waitState(procedureId, "RUNNING");
    stopProgram(procedureId);
    waitState(procedureId, "STOPPED");

    // removing apps
    Assert.assertEquals(200, doDelete("/v2/apps/WordCountApp").getStatusLine().getStatusCode());
  }

  /**
   * Metadata tests through appfabric apis.
   */
  @Test
  public void testGetMetadata() throws Exception {
    try {
      HttpResponse response = doPost("/v2/unrecoverable/reset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      response = deploy(WordCountApp.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      response = deploy(AppWithWorkflow.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      response = doGet("/v2/apps/WordCountApp/flows/WordCountFlow");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("WordCountFlow"));

      // verify procedure
      response = doGet("/v2/apps/WordCountApp/procedures/WordFrequency");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("WordFrequency"));

      //verify mapreduce
      response = doGet("/v2/apps/WordCountApp/mapreduce/VoidMapReduceJob");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("VoidMapReduceJob"));

      // verify single workflow
      response = doGet("/v2/apps/AppWithWorkflow/workflows/SampleWorkflow");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      result = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(result);
      Assert.assertTrue(result.contains("SampleWorkflow"));

      // verify apps
      response = doGet("/v2/apps");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(2, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "WordCountApp", "name", "WordCountApp",
                                                   "description", "Application for counting words")));
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "App", "id", "AppWithWorkflow", "name",
                                                   "AppWithWorkflow", "description", "Sample application")));

      // verify a single app
      response = doGet("/v2/apps/WordCountApp");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      Map<String, String> app = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
      Assert.assertEquals(ImmutableMap.of("type", "App", "id", "WordCountApp", "name", "WordCountApp",
                                          "description", "Application for counting words"), app);

      // verify flows
      response = doGet("/v2/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify flows by app
      response = doGet("/v2/apps/WordCountApp/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify procedures
      response = doGet("/v2/procedures");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCountApp", "id", "WordFrequency",
                                                   "name", "WordFrequency", "description",
                                                   "Procedure for executing WordFrequency.")));

      // verify procedures by app
      response = doGet("/v2/apps/WordCountApp/procedures");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Procedure", "app", "WordCountApp", "id", "WordFrequency",
                                                   "name", "WordFrequency", "description",
                                                   "Procedure for executing WordFrequency.")));


      // verify mapreduces
      response = doGet("/v2/mapreduce");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Mapreduce", "app", "WordCountApp", "id", "VoidMapReduceJob",
                                                   "name", "VoidMapReduceJob",
                                                   "description", "Mapreduce that does nothing " +
                                                   "(and actually doesn't run) - it is here for testing MDS")));

      // verify workflows
      response = doGet("/v2/workflows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of(
        "type", "Workflow", "app", "AppWithWorkflow", "id", "SampleWorkflow",
        "name", "SampleWorkflow", "description",  "SampleWorkflow description")));


      // verify programs by non-existent app
      response = doGet("/v2/apps/NonExistenyApp/flows");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());
      response = doGet("/v2/apps/NonExistenyApp/procedures");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());
      response = doGet("/v2/apps/NonExistenyApp/mapreduce");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());
      response = doGet("/v2/apps/NonExistenyApp/workflows");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());

      // verify programs by app that does not have that program type
      response = doGet("/v2/apps/AppWithWorkflow/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());
      response = doGet("/v2/apps/AppWithWorkflow/procedures");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());
      response = doGet("/v2/apps/AppWithWorkflow/mapreduce");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());
      response = doGet("/v2/apps/WordCountApp/workflows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());

      // verify flows by non-existent stream
      response = doGet("/v2/streams/nosuch/flows");
      Assert.assertEquals(404, response.getStatusLine().getStatusCode());

      // verify flows by stream
      response = doGet("/v2/streams/text/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify flows by dataset
      response = doGet("/v2/datasets/mydataset/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      Assert.assertTrue(o.contains(ImmutableMap.of("type", "Flow", "app", "WordCountApp", "id", "WordCountFlow", "name",
                                                   "WordCountFlow", "description", "Flow for counting words")));

      // verify flows by dataset not used by any flow
      response = doGet("/v2/datasets/input/flows");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertTrue(o.isEmpty());

      // verify one dataset
      response = doGet("/v2/datasets/mydataset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      Map<String, String> map = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
      Assert.assertNotNull(map);
      Assert.assertEquals("mydataset", map.get("id"));
      Assert.assertEquals("mydataset", map.get("name"));

      // verify all datasets
      response = doGet("/v2/datasets");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(3, o.size());
      Map<String, String> expectedDataSets = ImmutableMap.<String, String>builder()
        .put("input", ObjectStore.class.getName())
        .put("output", ObjectStore.class.getName())
        .put("mydataset", KeyValueTable.class.getName()).build();
      for (Map<String, String> ds : o) {
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
        Assert.assertEquals("problem with dataset " + ds.get("id"),
                            expectedDataSets.get(ds.get("id")), ds.get("classname"));
      }

      // verify datasets by app
      response = doGet("/v2/apps/WordCountApp/datasets");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      expectedDataSets = ImmutableMap.<String, String>builder()
        .put("mydataset", KeyValueTable.class.getName()).build();
      for (Map<String, String> ds : o) {
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("id"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("name"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), ds.containsKey("classname"));
        Assert.assertTrue("problem with dataset " + ds.get("id"), expectedDataSets.containsKey(ds.get("id")));
        Assert.assertEquals("problem with dataset " + ds.get("id"),
                            expectedDataSets.get(ds.get("id")), ds.get("classname"));
      }

      // verify one stream
      response = doGet("/v2/streams/text");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      map = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
      Assert.assertNotNull(map);
      Assert.assertEquals("text", map.get("id"));
      Assert.assertEquals("text", map.get("name"));
      Assert.assertNotNull(map.get("specification"));
      StreamSpecification sspec = new Gson().fromJson(map.get("specification"), StreamSpecification.class);
      Assert.assertNotNull(sspec);

      // verify all streams
      response = doGet("/v2/streams");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(2, o.size());
      Set<String> expectedStreams = ImmutableSet.of("text", "stream");
      for (Map<String, String> stream : o) {
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
        Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
      }

      // verify streams by app
      response = doGet("/v2/apps/WordCountApp/streams");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      s = EntityUtils.toString(response.getEntity());
      o = new Gson().fromJson(s, LIST_MAP_STRING_STRING_TYPE);
      Assert.assertEquals(1, o.size());
      expectedStreams = ImmutableSet.of("text");
      for (Map<String, String> stream : o) {
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("id"));
        Assert.assertTrue("problem with stream " + stream.get("id"), stream.containsKey("name"));
        Assert.assertTrue("problem with dataset " + stream.get("id"), expectedStreams.contains(stream.get("id")));
      }
    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  /**
   * Tests procedure instances.
   */
  @Test
  public void testProcedureInstances () throws Exception {
    Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    Assert.assertEquals(200, doPost("/v2/unrecoverable/reset").getStatusLine().getStatusCode());

    HttpResponse response = deploy(WordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/WordCountApp/procedures/WordFrequency/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> result = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(1, Integer.parseInt(result.get("instances")));

    JsonObject json = new JsonObject();
    json.addProperty("instances", 10);

    response = doPut("/v2/apps/WordCountApp/procedures/WordFrequency/instances", json.toString());
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/WordCountApp/procedures/WordFrequency/instances");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    s = EntityUtils.toString(response.getEntity());
    result = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(10, Integer.parseInt(result.get("instances")));


    // Get instances for a non-existent procedure and app.
    response = doGet("/v2/apps/WordCountApp/procedures/XXXX/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doGet("/v2/apps/XXXX/procedures/WordFrequency/instances");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    // PUT instances for non-existent procedure and app.
    String payload = "{instances: 1}";
    response = doPut("/v2/apps/WordCountApp/procedures/XXXX/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());

    response = doPut("/v2/apps/XXXX/procedures/WordFrequency/instances", payload);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());


    Assert.assertEquals(200, doDelete("/v2/apps/WordCountApp").getStatusLine().getStatusCode());
  }

  private String getWebappStatus(String appId) throws Exception {
    HttpResponse response = doGet("/v2/apps/" + appId + "/" + "webapp" + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, MAP_STRING_STRING_TYPE);
    return o.get("status");
  }

  @Test
  public void testFlowRuntimeArgs() throws Exception {
    testRuntimeArgs(WordCountApp.class, createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow"));
  }

  @Test
  public void testWorkflowRuntimeArgs() throws Exception {
    testRuntimeArgs(SleepingWorkflowApp.class,
                    createProgramId(ProgramType.WORKFLOW, "SleepWorkflowApp", "SleepWorkflow"));
  }

  @Test
  public void testProcedureRuntimeArgs() throws Exception {
    testRuntimeArgs(WordCountApp.class, createProgramId(ProgramType.PROCEDURE, "WordCountApp", "WordFrequency"));
  }

  @Test
  public void testMapreduceRuntimeArgs() throws Exception {
    testRuntimeArgs(DummyAppWithTrackingTable.class, createProgramId(ProgramType.MAPREDUCE, "dummy", "dummy-batch"));
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeploy() throws Exception {
    HttpResponse response = deploy(WordCountApp.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests taking a snapshot of the transaction manager.
   */
  @Test
  public void testTxManagerSnapshot() throws Exception {
    Long currentTs = System.currentTimeMillis();

    HttpResponse response = doGet("/v2/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    InputStream in = response.getEntity().getContent();
    SnapshotCodec snapshotCodec = getInjector().getInstance(SnapshotCodecProvider.class);
    try {
      TransactionSnapshot snapshot = snapshotCodec.decode(in);
      Assert.assertTrue(snapshot.getTimestamp() >= currentTs);
    } finally {
      in.close();
    }
  }

  /**
   * Tests invalidating a transaction.
   * @throws Exception
   */
  @Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient txClient = AppFabricTestBase.getTxClient();

    Transaction tx1 = txClient.startShort();
    HttpResponse response = doPost("/v2/transactions/" + tx1.getWritePointer() + "/invalidate");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Transaction tx2 = txClient.startShort();
    txClient.commit(tx2);
    response = doPost("/v2/transactions/" + tx2.getWritePointer() + "/invalidate");
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    Assert.assertEquals(400,
                        doPost("/v2/transactions/foobar/invalidate").getStatusLine().getStatusCode());
  }

  @Test
  public void testResetTxManagerState() throws Exception {
    HttpResponse response = doPost("/v2/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    // todo: first transaction after reset will fail, doGet() is a placeholder, can remove after tephra tx-fix
    doGet("/v2/apps");
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeployInvalid() throws Exception {
    HttpResponse response = deploy(String.class);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
    Assert.assertTrue(response.getEntity().getContentLength() > 0);
  }

  /**
   * Tests deploying an application with dataset same name as existing dataset but a different type
   */
  @Test
  public void testDeployFailure() throws Exception {
    HttpResponse response = deploy(AppWithDataset.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());

    response = deploy(AppWithDatasetDuplicate.class);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    Assert.assertNotNull(response.getEntity());
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDelete() throws Exception {
    //Delete an invalid app
    HttpResponse response = doDelete("/v2/apps/XYZ");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    deploy(WordCountApp.class);
    Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");
    startProgram(flowId);
    waitState(flowId, "RUNNING");

    //Try to delete an App while its flow is running
    response = doDelete("/v2/apps/WordCountApp");
    Assert.assertEquals(403, response.getStatusLine().getStatusCode());
    stopProgram(flowId);

    waitState(flowId, "STOPPED");
    //Delete the App after stopping the flow
    response = doDelete("/v2/apps/WordCountApp");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete("/v2/apps/WordCountApp");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests for program list calls
   */
  @Test
  public void testProgramList() throws Exception {
    //Test :: /flows /procedures /mapreduce /workflows
    //App  :: /apps/AppName/flows /procedures /mapreduce /workflows
    //App Info :: /apps/AppName
    //All Apps :: /apps

    HttpResponse response = doGet("/v2/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    deploy(WordCountApp.class);
    deploy(DummyAppWithTrackingTable.class);
    response = doGet("/v2/apps/WordCountApp/flows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> flows = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, flows.size());

    response = doGet("/v2/apps/WordCountApp/procedures");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> procedures = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, procedures.size());

    response = doGet("/v2/apps/WordCountApp/mapreduce");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> mapreduce = new Gson().fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    Assert.assertEquals(1, mapreduce.size());

    response = doGet("/v2/apps/WordCountApp/workflows");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet("/v2/apps");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doDelete("/v2/apps/dummy");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void setAndTestRuntimeArgs(Id.Program programId, Map<String, String> args) throws Exception {
    HttpResponse response;
    String argString = GSON.toJson(args, new TypeToken<Map<String, String>>() { }.getType());
    String runtimeArgsUrl = String.format("/v2/apps/%s/%s/%s/runtimeargs",
                                          programId.getApplicationId(),
                                          programId.getType().getCategoryName(), programId.getId());
    response = doPut(runtimeArgsUrl, argString);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    response = doGet(runtimeArgsUrl);

    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseEntity = EntityUtils.toString(response.getEntity());
    Map<String, String> argsRead = GSON.fromJson(responseEntity, new TypeToken<Map<String, String>>() { }.getType());

    Assert.assertEquals(args.size(), argsRead.size());
  }

  /**
   * Test for schedule handlers.
   */
  @Category(XSlowTests.class)
  @Test
  public void testScheduleEndPoints() throws Exception {
    // Steps for the test:
    // 1. Deploy the app
    // 2. Verify the schedules
    // 3. Verify the history after waiting a while
    // 4. Suspend the schedule
    // 5. Verify there are no runs after the suspend by looking at the history
    // 6. Resume the schedule
    // 7. Verify there are runs after the resume by looking at the history

    HttpResponse response = deploy(AppWithSchedule.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Id.Program workflowId = createProgramId(ProgramType.WORKFLOW, "AppWithSchedule", "SampleWorkflow");

    response = doPost(String.format("/v2/apps/AppWithSchedule/schedules/SampleSchedule/resume"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Map<String, String> runtimeArguments = Maps.newHashMap();
    runtimeArguments.put("someKey", "someWorkflowValue");
    runtimeArguments.put("workflowKey", "workflowValue");

    setAndTestRuntimeArgs(workflowId, runtimeArguments);

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/schedules");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String json = EntityUtils.toString(response.getEntity());
    List<ScheduleSpecification> schedules =
      GSON.fromJson(json, new TypeToken<List<ScheduleSpecification>>() { }.getType());
    Assert.assertEquals(1, schedules.size());
    String scheduleName = schedules.get(0).getSchedule().getName();
    Assert.assertNotNull(scheduleName);
    Assert.assertFalse(scheduleName.isEmpty());

    scheduleHistoryRuns(5, "/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed", 0);

    //Check suspend status
    String scheduleStatus = String.format("/v2/apps/AppWithSchedule/schedules/%s/status",
                                          scheduleName);
    scheduleStatusCheck(5, scheduleStatus, "SCHEDULED");

    String scheduleSuspend = String.format("/v2/apps/AppWithSchedule/schedules/%s/suspend",
                                           scheduleName);

    response = doPost(scheduleSuspend);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    scheduleStatusCheck(5, scheduleStatus, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed");
    json = EntityUtils.toString(response.getEntity());
    List<Map<String, String>> history = GSON.fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    int workflowRuns = history.size();

    //Sleep for some time and verify there are no more scheduled jobs after the suspend.
    TimeUnit.SECONDS.sleep(10);

    response = doGet("/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed");
    json = EntityUtils.toString(response.getEntity());
    history = GSON.fromJson(json, LIST_MAP_STRING_STRING_TYPE);
    int workflowRunsAfterSuspend = history.size();
    Assert.assertEquals(workflowRuns, workflowRunsAfterSuspend);

    String scheduleResume = String.format("/v2/apps/AppWithSchedule/schedules/%s/resume",
                                          scheduleName);

    response = doPost(scheduleResume);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    scheduleHistoryRuns(5, "/v2/apps/AppWithSchedule/workflows/SampleWorkflow/runs?status=completed",
                        workflowRunsAfterSuspend);

    //check scheduled state
    scheduleStatusCheck(5, scheduleStatus, "SCHEDULED");

    //Check status of a non existing schedule
    String notFoundSchedule = String.format("/v2/apps/AppWithSchedule/schedules/%s/status", "invalidId");

    scheduleStatusCheck(5, notFoundSchedule, "NOT_FOUND");

    response = doPost(scheduleSuspend);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    //check paused state
    scheduleStatusCheck(5, scheduleStatus, "SUSPENDED");

    TimeUnit.SECONDS.sleep(2); //wait till any running jobs just before suspend call completes.

    response = doDelete("/v2/apps/AppWithSchedule");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Test for resetting app.
   */
  @Test
  public void testUnRecoverableReset() throws Exception {
    try {
      HttpResponse response = deploy(WordCountApp.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      response = doPost("/v2/unrecoverable/reset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
    // make sure that after reset (no apps), list apps returns 200, and not 404
    Assert.assertEquals(200, doGet("/v2/apps").getStatusLine().getStatusCode());
  }

  /**
   * Test for resetting app.
   */
  @Test
  public void testUnRecoverableDatasetsDeletion() throws Exception {
    try {
      // deploy app and create datasets
      deploy(WordCountApp.class);
      Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");
      startProgram(flowId);
      waitState(flowId, "RUNNING");

      // check that datasets were created
      Assert.assertTrue(
        GSON.fromJson(EntityUtils.toString(doGet("/v2/datasets").getEntity()), JsonArray.class).size() > 0);

      // try delete datasets while program is running: should fail
      HttpResponse response = doDelete("/v2/unrecoverable/data/datasets");
      Assert.assertEquals(400, response.getStatusLine().getStatusCode());

      // stop program
      stopProgram(flowId);
      waitState(flowId, "STOPPED");

      // verify delete all datasets succeeded
      response = doDelete("/v2/unrecoverable/data/datasets");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());

      // check there are no datasets
      Assert.assertEquals(
        0, GSON.fromJson(EntityUtils.toString(doGet("/v2/datasets").getEntity()), JsonArray.class).size());

    } finally {
      Assert.assertEquals(200, doDelete("/v2/apps").getStatusLine().getStatusCode());
    }
    // make sure that after reset (no apps), list apps returns 200, and not 404
    Assert.assertEquals(200, doGet("/v2/apps").getStatusLine().getStatusCode());
  }

  @Test
  public void testHistoryDeleteAfterUnrecoverableReset() throws Exception {

    Id.Program programId = createProgramId(ProgramType.MAPREDUCE, "dummy", "dummy-batch");

    deploy(DummyAppWithTrackingTable.class);

    // first run
    startProgram(programId);
    waitState(programId, "RUNNING");
    stopProgram(programId);
    waitState(programId, "STOPPED");

    // verify the run by checking if history has one entry
    verifyHistories(programId, 1, "killed");

    HttpResponse response = doPost("/v2/unrecoverable/reset");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    // Verify that the unrecoverable reset deletes the history
    verifyHistories(programId, 0, null);
  }


  /**
   * Test for resetting app.
   */
  @Test
  public void testUnRecoverableResetAppRunning() throws Exception {

    HttpResponse response = deploy(WordCountApp.class);
    Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    startProgram(flowId);
    waitState(flowId, "RUNNING");
    response = doPost("/v2/unrecoverable/reset");
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());
    stopProgram(flowId);
    waitState(flowId, "STOPPED");
  }

  @Test
  public void testBatchStatus() throws Exception {
    String url = "/v2/status";
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    Assert.assertEquals(400, doPost(url, "").getStatusLine().getStatusCode());
    // empty array is valid args
    Assert.assertEquals(200, doPost(url, "[]").getStatusLine().getStatusCode());
    deploy(WordCountApp.class);
    deploy(AppWithServices.class);
    // data requires appId, programId, and programType. Test missing fields/invalid programType
    Assert.assertEquals(400, doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(url, "[{'appId':'WordCountApp', 'programId':'WordCountFlow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(url, "[{'programType':'Flow', 'programId':'WordCountFlow'}, {'appId':" +
      "'AppWithServices', 'programType': 'service', 'programId': 'NoOpService'}]").getStatusLine().getStatusCode());
    Assert.assertEquals(400,
                        doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow' 'programId':'WordCountFlow'}]")
      .getStatusLine().getStatusCode());
    // Test missing app, programType, etc
    List<JsonObject> returnedBody = readResponse(
      doPost(url, "[{'appId':'NotExist', 'programType':'Flow', 'programId':'WordCountFlow'}]"), typeToken);
    Assert.assertEquals("App: NotExist not found", returnedBody.get(0).get("error").getAsString());
    returnedBody = readResponse(
      doPost(url, "[{'appId':'WordCountApp', 'programType':'flow', 'programId':'NotExist'}," +
        "{'appId':'WordCountApp', 'programType':'flow', 'programId':'WordCountFlow'}]"), typeToken);
    Assert.assertEquals("Program not found", returnedBody.get(0).get("error").getAsString());
    // The programType should be consistent. Second object should have proper status
    Assert.assertEquals("Flow", returnedBody.get(1).get("programType").getAsString());
    Assert.assertEquals("STOPPED", returnedBody.get(1).get("status").getAsString());
    HttpResponse response = doPost(url,
                                   "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': 'WordFrequency'}," +
      "{'appId': 'WordCountApp', 'programType': 'Mapreduce', 'programId': 'VoidMapReduceJob'}]");
    // test valid cases
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, typeToken);
    for (JsonObject obj : returnedBody) {
      Assert.assertEquals(200, obj.get("statusCode").getAsInt());
      Assert.assertEquals("STOPPED", obj.get("status").getAsString());
    }
    // start the flow
    Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");
    startProgram(flowId);
    response = doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, typeToken);
    Assert.assertEquals("RUNNING", returnedBody.get(0).get("status").getAsString());

    // start the service
    Id.Program serviceId = createProgramId(ProgramType.SERVICE, "AppWithServices", "NoOpService");
    startProgram(serviceId);

    response = doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'AppWithServices', 'programType': 'Service', 'programId': 'NoOpService'}," +
      "{'appId': 'WordCountApp', 'programType': 'Mapreduce', 'programId': 'VoidMapReduceJob'}]");
    // test valid cases
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, typeToken);
    Assert.assertEquals("RUNNING", returnedBody.get(0).get("status").getAsString());
    Assert.assertEquals("RUNNING", returnedBody.get(1).get("status").getAsString());
    Assert.assertEquals("STOPPED", returnedBody.get(2).get("status").getAsString());

    stopProgram(flowId);
    stopProgram(serviceId);
    waitState(flowId, "STOPPED");
    waitState(serviceId, "STOPPED");

  }

  @Test
  public void testBatchInstances() throws Exception {
    String url = "/v2/instances";
    Type typeToken = new TypeToken<List<JsonObject>>() { }.getType();
    Assert.assertEquals(400, doPost(url, "").getStatusLine().getStatusCode());
    // empty array is valid args
    Assert.assertEquals(200, doPost(url, "[]").getStatusLine().getStatusCode());
    deploy(WordCountApp.class);
    deploy(AppWithServices.class);
    // data requires appId, programId, and programType. Test missing fields/invalid programType
    Assert.assertEquals(400, doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(url, "[{'appId':'WordCountApp', 'programId':'WordCountFlow'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(url, "[{'programType':'Flow', 'programId':'WordCountFlow'}," +
      "{'appId': 'WordCountApp', 'programType': 'procedure', 'programId': 'WordFrequency'}]")
      .getStatusLine().getStatusCode());
    Assert.assertEquals(400, doPost(url, "[{'appId':'WordCountApp', 'programType':'NotExist', " +
      "'programId':'WordCountFlow'}]").getStatusLine().getStatusCode());
    // Test malformed json
    Assert.assertEquals(400,
                        doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow' 'programId':'WordCountFlow'}]")
                          .getStatusLine().getStatusCode());
    // Test missing app, programType, etc
    List<JsonObject> returnedBody = readResponse(
      doPost(url, "[{'appId':'NotExist', 'programType':'Flow', 'programId':'WordCountFlow'}]"), typeToken);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());
    returnedBody = readResponse(
      doPost(url, "[{'appId':'WordCountApp', 'programType':'flow', 'programId':'WordCountFlow', 'runnableId': " +
        "NotExist'}]"), typeToken);
    Assert.assertEquals(404, returnedBody.get(0).get("statusCode").getAsInt());
    HttpResponse response = doPost(url,
      "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow', 'runnableId': 'StreamSource'}," +
      "{'appId': 'WordCountApp', 'programType': 'Procedure', 'programId': 'WordFrequency'}," +
      "{'appId': 'AppWithServices', 'programType':'Service', 'programId':'NoOpService', 'runnableId':'NoOpService'}]");
    // test valid cases
    returnedBody = readResponse(response, typeToken);
    for (JsonObject obj : returnedBody) {
      Assert.assertEquals(200, obj.get("statusCode").getAsInt());
      Assert.assertEquals(1, obj.get("requested").getAsInt());
      Assert.assertEquals(0, obj.get("provisioned").getAsInt());
    }
    // start the flow
    Id.Program flowId = createProgramId(ProgramType.FLOW, "WordCountApp", "WordCountFlow");
    startProgram(flowId);

    response = doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow', 'programId':'WordCountFlow'," +
      "'runnableId': 'StreamSource'}]");
    returnedBody = readResponse(response, typeToken);
    Assert.assertEquals(1, returnedBody.get(0).get("provisioned").getAsInt());

    // start the service
    Id.Program serviceId = createProgramId(ProgramType.SERVICE, "AppWithServices", "NoOpService");
    startProgram(serviceId);
    response = doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow','programId':'WordCountFlow','runnableId':" +
      "'StreamSource'}, {'appId':'AppWithServices', 'programType':'Service','programId':'NoOpService', 'runnableId':" +
      "'NoOpService'}, {'appId': 'WordCountApp', 'programType': 'Procedure','programId': 'VoidMapReduceJob'}]");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    returnedBody = readResponse(response, typeToken);
    Assert.assertEquals(1, returnedBody.get(0).get("provisioned").getAsInt());
    Assert.assertEquals(1, returnedBody.get(1).get("provisioned").getAsInt());
    // does not exist
    Assert.assertEquals(404, returnedBody.get(2).get("statusCode").getAsInt());

    setFlowletInstances(flowId, "StreamSource", 2);

    returnedBody = readResponse(doPost(url, "[{'appId':'WordCountApp', 'programType':'Flow'," +
      "'programId':'WordCountFlow', 'runnableId': 'StreamSource'}]"), typeToken);
    Assert.assertEquals(2, returnedBody.get(0).get("requested").getAsInt());

    stopProgram(flowId);
    stopProgram(serviceId);

    waitState(flowId, "STOPPED");
    waitState(serviceId, "STOPPED");
  }

  @Test
  public void testServiceSpecification() throws Exception {
    deploy(AppWithServices.class);
    HttpResponse response = doGet("/v2/apps/AppWithServices/services/NoOpService/");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Set<ServiceHttpEndpoint> expectedEndpoints = ImmutableSet.of(new ServiceHttpEndpoint("GET", "/ping"),
                                                                 new ServiceHttpEndpoint("POST", "/multi"),
                                                                 new ServiceHttpEndpoint("GET", "/multi"),
                                                                 new ServiceHttpEndpoint("GET", "/multi/ping"));

    GsonBuilder gsonBuidler = new GsonBuilder();
    gsonBuidler.registerTypeAdapter(ServiceSpecification.class, new ServiceSpecificationCodec());
    gsonBuidler.registerTypeAdapter(HttpServiceHandlerSpecification.class, new HttpServiceSpecificationCodec());
    Gson gson = gsonBuidler.create();
    ServiceSpecification specification = readResponse(response, ServiceSpecification.class, gson);

    Set<ServiceHttpEndpoint> returnedEndpoints = Sets.newHashSet();
    for (HttpServiceHandlerSpecification httpServiceHandlerSpecification : specification.getHandlers().values()) {
      returnedEndpoints.addAll(httpServiceHandlerSpecification.getEndpoints());
    }

    Assert.assertEquals("NoOpService", specification.getName());
    Assert.assertTrue(returnedEndpoints.equals(expectedEndpoints));
    Assert.assertEquals(0, specification.getWorkers().values().size());
  }

  @Test
  public void testWorkflowSpecification() throws Exception {
    deploy(WorkflowApp.class);
    HttpResponse response = doGet("/v2/apps/WorkflowApp/workflows/FunWorkflow/");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Gson gson = new GsonBuilder().registerTypeAdapter(WorkflowActionSpecification.class,
                                                                    new WorkflowActionSpecificationCodec()).create();

    String responseString = readResponse(response);

    JsonObject jsonObj = new JsonParser().parse(responseString).getAsJsonObject();

    Assert.assertTrue(jsonObj.get("className").getAsString().contains("FunWorkflow"));
    Assert.assertEquals("FunWorkflow", jsonObj.get("name").getAsString());
    Assert.assertEquals("FunWorkflow description", jsonObj.get("description").getAsString());

    Type actionType = new TypeToken<List<ScheduleProgramInfo>>() { }.getType();
    List<ScheduleProgramInfo> actions = gson.fromJson(jsonObj.get("actions").getAsJsonArray(), actionType);

    Assert.assertNotNull(actions);
    Assert.assertEquals(3, actions.size());

    Assert.assertEquals(actions.get(0), new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "ClassicWordCount"));
    Assert.assertEquals(actions.get(1), new ScheduleProgramInfo(SchedulableProgramType.CUSTOM_ACTION, "verify"));
    Assert.assertEquals(actions.get(2), new ScheduleProgramInfo(SchedulableProgramType.SPARK, "SparkWorkflowTest"));

    Type mapType = new TypeToken<Map<String, WorkflowActionSpecification>>() { }.getType();
    Map<String, WorkflowActionSpecification> customActionMap = gson.fromJson(jsonObj.get("customActionMap")
                                                                               .getAsJsonObject(), mapType);
    Assert.assertNotNull(customActionMap);
    Assert.assertEquals(1, customActionMap.size());

    Assert.assertTrue(customActionMap.containsKey("verify"));
    WorkflowActionSpecification actionSpec = customActionMap.get("verify");
    Assert.assertEquals("verify", actionSpec.getName());
    Assert.assertEquals(1, actionSpec.getProperties().size());
  }
}
