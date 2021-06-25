package io.temporal.internal.sync

import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.workflowservice.v1.PollWorkflowTaskQueueResponse
import io.temporal.common.converter.DataConverter
import io.temporal.common.interceptors.WorkerInterceptor
import io.temporal.internal.common.InternalUtils
import io.temporal.internal.common.WorkflowExecutionHistory
import io.temporal.internal.replay.ReplayWorkflowTaskHandler
import io.temporal.internal.replay.WorkflowExecutorCache
import io.temporal.internal.worker.LocalActivityWorker
import io.temporal.internal.worker.SingleWorkerOptions
import io.temporal.internal.worker.WorkflowTaskHandler
import io.temporal.internal.worker.WorkflowWorker
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.worker.WorkflowImplementationOptions
import io.temporal.workflow.Functions.Func
import java.lang.Exception
import java.lang.reflect.Type
import java.time.Duration
import java.util.Objects
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class CoroutineWorkflowWorker(
  service: WorkflowServiceStubs?,
  namespace: String?,
  taskQueue: String?,
  workerInterceptors: Array<WorkerInterceptor?>?,
  singleWorkerOptions: SingleWorkerOptions,
  localActivityOptions: SingleWorkerOptions,
  cache: WorkflowExecutorCache?,
  stickyTaskQueueName: String?,
  stickyWorkflowTaskScheduleToStartTimeout: Duration?,
  workflowThreadPool: ThreadPoolExecutor
) : ISyncWorkflowWorker {

  private val workflowWorker: WorkflowWorker
  private val laWorker: LocalActivityWorker
  private var factory: POJOWorkflowImplementationFactory
  private val dataConverter: DataConverter
  private val laTaskHandler: POJOActivityTaskHandler
  private val heartbeatExecutor = Executors.newScheduledThreadPool(4)

  init {
    Objects.requireNonNull(workflowThreadPool)
    dataConverter = singleWorkerOptions.dataConverter
    factory = POJOWorkflowImplementationFactory(
      singleWorkerOptions, workflowThreadPool, workerInterceptors, cache
    )
    laTaskHandler = POJOActivityTaskHandler(
      service,
      namespace,
      localActivityOptions.dataConverter,
      heartbeatExecutor,
      workerInterceptors
    )
    laWorker = LocalActivityWorker(namespace, taskQueue, localActivityOptions, laTaskHandler)
    val taskHandler: WorkflowTaskHandler = ReplayWorkflowTaskHandler(
      namespace,
      factory,
      cache,
      singleWorkerOptions,
      stickyTaskQueueName,
      stickyWorkflowTaskScheduleToStartTimeout,
      service, { this.isShutdown },
      laWorker.getLocalActivityTaskPoller()
    )
    workflowWorker = WorkflowWorker(
      service, namespace, taskQueue, singleWorkerOptions, taskHandler, stickyTaskQueueName
    )
  }

  override fun registerWorkflowImplementationTypes(
    options: WorkflowImplementationOptions?, workflowImplementationTypes: Array<Class<*>?>?
  ) {
    factory.registerWorkflowImplementationTypes(options, workflowImplementationTypes)
  }

  override fun <R> addWorkflowImplementationFactory(
    options: WorkflowImplementationOptions?, clazz: Class<R>?, factory: Func<R>?
  ) {
    this.factory.addWorkflowImplementationFactory(options, clazz, factory)
  }

  override fun <R> addWorkflowImplementationFactory(clazz: Class<R>?, factory: Func<R>?) {
    this.factory.addWorkflowImplementationFactory(clazz, factory)
  }

  override fun registerLocalActivityImplementations(vararg activitiesImplementation: Any?) {
    laTaskHandler.registerLocalActivityImplementations(activitiesImplementation)
  }

  override fun start() {
    workflowWorker.start()
    // It doesn't start if no types are registered with it.
    if (workflowWorker.isStarted) {
      laWorker.start()
    }
  }

  override fun isStarted(): Boolean {
    return workflowWorker.isStarted && (laWorker.isStarted || !laWorker.isAnyTypeSupported)
  }

  override fun isShutdown(): Boolean {
    return workflowWorker.isShutdown && laWorker.isShutdown
  }

  override fun isTerminated(): Boolean {
    return workflowWorker.isTerminated && laWorker.isTerminated
  }

  override fun shutdown() {
    laWorker.shutdown()
    workflowWorker.shutdown()
  }

  override fun shutdownNow() {
    laWorker.shutdownNow()
    workflowWorker.shutdownNow()
  }

  override fun awaitTermination(timeout: Long, unit: TimeUnit) {
    val timeoutMillis = InternalUtils.awaitTermination(laWorker, unit.toMillis(timeout))
    InternalUtils.awaitTermination(workflowWorker, timeoutMillis)
  }

  override fun suspendPolling() {
    workflowWorker.suspendPolling()
    laWorker.suspendPolling()
  }

  override fun resumePolling() {
    workflowWorker.resumePolling()
    laWorker.resumePolling()
  }

  override fun isSuspended(): Boolean {
    return workflowWorker.isSuspended && laWorker.isSuspended
  }

  @Throws(Exception::class) override fun <R> queryWorkflowExecution(
    execution: WorkflowExecution?,
    queryType: String?,
    resultClass: Class<R>?,
    resultType: Type?,
    args: Array<Any?>
  ): R {
    val serializedArgs = dataConverter.toPayloads(*args)
    val result = workflowWorker.queryWorkflowExecution(execution, queryType, serializedArgs)
    return dataConverter.fromPayloads(0, result, resultClass, resultType)
  }

  @Throws(Exception::class) override fun <R> queryWorkflowExecution(
    history: WorkflowExecutionHistory?,
    queryType: String?,
    resultClass: Class<R>?,
    resultType: Type?,
    args: Array<Any?>
  ): R {
    val serializedArgs = dataConverter.toPayloads(*args)
    val result = workflowWorker.queryWorkflowExecution(history, queryType, serializedArgs)
    return dataConverter.fromPayloads(0, result, resultClass, resultType)
  }

  override fun apply(pollWorkflowTaskQueueResponse: PollWorkflowTaskQueueResponse?) {
    workflowWorker.apply(pollWorkflowTaskQueueResponse)
  }
}