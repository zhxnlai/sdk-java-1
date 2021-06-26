package io.temporal.internal.sync

import io.temporal.common.context.ContextPropagator
import io.temporal.common.converter.DataConverter
import io.temporal.internal.replay.ReplayWorkflow
import io.temporal.worker.WorkflowImplementationOptions

internal interface SyncReplayWorkflowFactory {

  fun getWorkflow(
    workflow: SyncWorkflowDefinition,
    options: WorkflowImplementationOptions?,
    dataConverter: DataConverter,
    contextPropagators: List<ContextPropagator>
  ): ReplayWorkflow
}