package io.temporal.internal.sync;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponse;
import io.temporal.internal.common.WorkflowExecutionHistory;
import io.temporal.internal.worker.SuspendableWorker;
import io.temporal.worker.WorkflowImplementationOptions;
import io.temporal.workflow.Functions;
import java.lang.reflect.Type;

public interface ISyncWorkflowWorker
    extends SuspendableWorker, Functions.Proc1<PollWorkflowTaskQueueResponse> {
  void registerWorkflowImplementationTypes(
      WorkflowImplementationOptions options, Class<?>[] workflowImplementationTypes);

  <R> void addWorkflowImplementationFactory(
      WorkflowImplementationOptions options, Class<R> clazz, Functions.Func<R> factory);

  <R> void addWorkflowImplementationFactory(Class<R> clazz, Functions.Func<R> factory);

  void registerLocalActivityImplementations(Object... activitiesImplementation);

  <R> R queryWorkflowExecution(
      WorkflowExecution execution,
      String queryType,
      Class<R> resultClass,
      Type resultType,
      Object[] args)
      throws Exception;

  <R> R queryWorkflowExecution(
      WorkflowExecutionHistory history,
      String queryType,
      Class<R> resultClass,
      Type resultType,
      Object[] args)
      throws Exception;
}
