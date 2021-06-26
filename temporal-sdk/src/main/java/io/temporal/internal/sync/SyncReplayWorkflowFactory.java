package io.temporal.internal.sync;

import io.temporal.common.context.ContextPropagator;
import io.temporal.common.converter.DataConverter;
import io.temporal.internal.replay.ReplayWorkflow;
import io.temporal.worker.WorkflowImplementationOptions;
import java.util.List;

public interface SyncReplayWorkflowFactory {

  ReplayWorkflow getWorkflow(
      SyncWorkflowDefinition workflow,
      WorkflowImplementationOptions options,
      DataConverter dataConverter,
      List<ContextPropagator> contextPropagators
  );
}
