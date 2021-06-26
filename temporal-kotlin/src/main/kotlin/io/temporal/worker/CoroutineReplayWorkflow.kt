package io.temporal.worker

import io.temporal.api.common.v1.Payloads
import io.temporal.api.enums.v1.EventType
import io.temporal.api.history.v1.HistoryEvent
import io.temporal.api.query.v1.WorkflowQuery
import io.temporal.client.WorkflowClient
import io.temporal.common.context.ContextPropagator
import io.temporal.common.converter.DataConverter
import io.temporal.internal.replay.ReplayWorkflow
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.internal.sync.POJOWorkflowImplementationFactory
import io.temporal.internal.sync.SyncReplayWorkflowFactory
import io.temporal.internal.sync.SyncWorkflowContext
import io.temporal.internal.sync.SyncWorkflowDefinition
import io.temporal.internal.sync.WorkflowExecuteRunnable
import io.temporal.internal.sync.WorkflowInternal
import io.temporal.internal.worker.WorkflowExecutionException
import io.temporal.workflow.WorkflowInfo
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import java.util.Optional
import java.util.concurrent.CancellationException
import kotlin.coroutines.EmptyCoroutineContext

object KWorkflow {
  @JvmStatic
  fun currentTimeMillis(): Long {
    return CoroutineReplayWorkflow.threadLocalDispatcher.get()?.currentTime ?: 0
  }

  @JvmStatic
  fun getInfo(): WorkflowInfo {

  }
}

object KWorkflowInternal {
}

class CoroutineReplayWorkflow(
  private val workflow: SyncWorkflowDefinition,
  private val options: WorkflowImplementationOptions,
  private val dataConverter: DataConverter,
  private val contextPropagators: List<ContextPropagator>,
) : ReplayWorkflow {

  companion object {
    val threadLocalDispatcher = ThreadLocal<DelayController>()
  }

  private lateinit var workflowProc: WorkflowExecuteRunnable
  private lateinit var deferred: Deferred<*>
  private lateinit var dispatcher: DelayController
  private lateinit var scope: TestCoroutineScope

  override fun start(event: HistoryEvent, context: ReplayWorkflowContext) {
    require(
      !(event.eventType != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        || !event.hasWorkflowExecutionStartedEventAttributes())
    ) { "first event is not WorkflowExecutionStarted, but " + event.eventType }
    val startEvent = event.workflowExecutionStartedEventAttributes

    val result = if (startEvent.hasLastCompletionResult()) Optional.of(startEvent.lastCompletionResult) else Optional.empty()
    val lastFailure = if (startEvent.hasContinuedFailure()) Optional.of(startEvent.continuedFailure) else Optional.empty()

    // TODO replace SyncWorkflowContext because it calls into WorkflowThread
    val syncContext = SyncWorkflowContext(
      context,
      workflowImplementationOptions,
      dataConverter,
      contextPropagators,
      result,
      lastFailure
    )

    workflowProc = WorkflowExecuteRunnable(syncContext, workflow, startEvent)

    // The following order is ensured by this code and DeterministicRunner implementation:
    // 1. workflow.initialize
    // 2. signal handler (if signalWithStart was called)
    // 3. main workflow method
    // runner = DeterministicRunner.newRunner(
    //   threadPool,
    //   syncContext,
    //   Runnable {
    //     workflow.initialize()
    //     WorkflowInternal.newThread(
    //       false,
    //       DeterministicRunnerImpl.WORKFLOW_ROOT_THREAD_NAME
    //     ) { workflowProc.run() }
    //       .start()
    //   },
    //   cache
    // )
    // TODO tracing
    // runner.setInterceptorHead(syncContext.getWorkflowInterceptor())

    val (safeContext, dispatcher) = EmptyCoroutineContext.checkArguments()
    this.dispatcher = dispatcher
    threadLocalDispatcher.set(this.dispatcher)
    scope = TestCoroutineScope(safeContext)
    deferred = scope.async {
      workflow.initialize()
      async {
        workflowProc.run()
      }
    }
    // val startingJobs = safeContext.activeJobs()
    // val endingJobs = safeContext.activeJobs()
    // if ((endingJobs - startingJobs).isNotEmpty()) {
    //   throw UncompletedCoroutinesError("Test finished with active jobs: $endingJobs")
    // }
  }

  override fun handleSignal(signalName: String, input: Optional<Payloads>, eventId: Long) {
    /**
     * TODO:
     * Executes a runnable in a specially created workflow thread. This newly created thread is given
     * chance to run before any other existing threads. This is used to ensure that some operations
     * (like signal callbacks) are executed before all other threads which is important to guarantee
     * their processing even if they were received after workflow code decided to complete. To be
     * called before runUntilAllBlocked.
     */
    scope.async {
      workflowProc.handleSignal(signalName, input, eventId)
    }
  }

  override fun eventLoop(): Boolean {
    // TODO getDeadlockDetectionTimeout()
    dispatcher.advanceUntilIdle()
    deferred.getCompletionExceptionOrNull()?.let {
      throw it
    }
    scope.cleanupTestCoroutines()
    return workflowProc.isDone()
  }

  override fun getOutput(): Optional<Payloads> {
    return workflowProc.output
  }

  override fun cancel(reason: String) {
    if (::deferred.isInitialized) {
      deferred.cancel(CancellationException(reason))
    }
    threadLocalDispatcher.set(null)
  }

  override fun close() {
    if (::deferred.isInitialized) {
      deferred.cancel()
    }
    threadLocalDispatcher.set(null)
  }

  override fun query(query: WorkflowQuery): Optional<Payloads> {
    if (WorkflowClient.QUERY_TYPE_REPLAY_ONLY == query.queryType) {
      return Optional.empty()
    }
    if (WorkflowClient.QUERY_TYPE_STACK_TRACE == query.queryType) {
      return dataConverter.toPayloads("runner.stackTrace()")
    }
    val args = if (query.hasQueryArgs()) Optional.of(query.queryArgs) else Optional.empty()
    return workflowProc.handleQuery(query.queryType, args)
  }

  override fun mapUnexpectedException(failure: Throwable): WorkflowExecutionException {
    return POJOWorkflowImplementationFactory.mapToWorkflowExecutionException(
      failure, dataConverter
    )
  }

  override fun getWorkflowImplementationOptions(): WorkflowImplementationOptions {
    return options
  }

  class Factory : SyncReplayWorkflowFactory {

    override fun getWorkflow(workflow: SyncWorkflowDefinition, options: WorkflowImplementationOptions?, dataConverter: DataConverter, contextPropagators: List<ContextPropagator>): ReplayWorkflow {
      return CoroutineReplayWorkflow(
        workflow,
        options ?: WorkflowImplementationOptions.newBuilder().build(),
        dataConverter,
        contextPropagators,
      )
    }
  }
}