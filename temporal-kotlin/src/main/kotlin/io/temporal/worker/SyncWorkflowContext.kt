/*
 *  Copyright (C) 2020 Temporal Technologies, Inc. All Rights Reserved.
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package io.temporal.worker

import com.uber.m3.tally.Scope
import io.temporal.worker.KWorkflow.currentTimeMillis
import io.temporal.internal.replay.ReplayWorkflowContext
import io.temporal.worker.WorkflowImplementationOptions
import io.temporal.common.converter.DataConverter
import io.temporal.common.context.ContextPropagator
import io.temporal.api.common.v1.Payloads
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor
import io.temporal.internal.sync.SignalDispatcher
import io.temporal.internal.sync.QueryDispatcher
import io.temporal.activity.ActivityOptions
import java.util.HashMap
import java.util.function.BiConsumer
import java.util.function.BiFunction
import io.temporal.workflow.Functions.Func1
import io.temporal.internal.replay.ExecuteActivityParameters
import io.temporal.workflow.Functions.Proc1
import io.temporal.workflow.Functions.Proc2
import io.temporal.workflow.CancellationScope
import io.temporal.failure.CanceledFailure
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor.SignalInput
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor
import io.temporal.failure.FailureConverter
import java.lang.RuntimeException
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.LocalActivityInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.LocalActivityOutput
import io.temporal.worker.KWorkflow
import io.temporal.activity.LocalActivityOptions
import io.temporal.internal.replay.ExecuteLocalActivityParameters
import io.temporal.workflow.Functions.Proc
import io.temporal.api.command.v1.ScheduleActivityTaskCommandAttributes
import io.temporal.api.common.v1.ActivityType
import io.temporal.internal.common.ProtobufTimeUtils
import io.temporal.common.RetryOptions
import io.temporal.internal.common.SerializerUtils
import io.temporal.internal.common.HeaderUtils
import io.temporal.api.workflowservice.v1.PollActivityTaskQueueResponse
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.ChildWorkflowInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.ChildWorkflowOutput
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.api.command.v1.StartChildWorkflowExecutionCommandAttributes
import io.temporal.api.common.v1.WorkflowType
import io.temporal.internal.common.OptionsUtils
import io.temporal.internal.common.InternalUtils
import io.temporal.api.common.v1.Memo
import io.temporal.api.enums.v1.ParentClosePolicy
import io.temporal.internal.replay.StartChildWorkflowExecutionParameters
import java.util.concurrent.atomic.AtomicBoolean
import io.temporal.api.common.v1.Payload
import io.temporal.failure.TemporalFailure
import io.temporal.client.WorkflowException
import io.temporal.failure.ChildWorkflowFailure
import io.temporal.internal.replay.ChildWorkflowTaskFailedException
import java.lang.IllegalArgumentException
import java.lang.Void
import io.temporal.workflow.Functions.Func
import java.util.function.BiPredicate
import java.util.concurrent.atomic.AtomicReference
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.RegisterQueryInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.RegisterSignalHandlersInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.RegisterDynamicSignalHandlerInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.RegisterDynamicQueryHandlerInput
import java.util.UUID
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.SignalExternalInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.SignalExternalOutput
import io.temporal.api.command.v1.SignalExternalWorkflowExecutionCommandAttributes
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.ContinueAsNewInput
import io.temporal.api.command.v1.ContinueAsNewWorkflowExecutionCommandAttributes
import io.temporal.workflow.ContinueAsNewOptions
import io.temporal.api.common.v1.SearchAttributes
import io.temporal.api.failure.v1.Failure
import io.temporal.api.taskqueue.v1.TaskQueue
import io.temporal.common.interceptors.Header
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.CancelWorkflowInput
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor.CancelWorkflowOutput
import io.temporal.workflow.Promise
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import java.lang.Error
import java.lang.Exception
import java.lang.Runnable
import java.lang.reflect.Type
import java.time.Duration
import java.util.Objects
import java.util.Optional
import java.util.Random
import java.util.function.Supplier

class SyncWorkflowContext(
  val context: ReplayWorkflowContext,
  workflowImplementationOptions: WorkflowImplementationOptions?,
  val dataConverter: DataConverter,
  private val contextPropagators: List<ContextPropagator>?,
  private val lastCompletionResult: Optional<Payloads?>?,
  val previousRunFailure: Optional<Failure?>?
) : WorkflowOutboundCallsInterceptor {
  private val signalDispatcher: SignalDispatcher
  private val queryDispatcher: QueryDispatcher
  private var headInterceptor: WorkflowOutboundCallsInterceptor? = null
  private var defaultActivityOptions: ActivityOptions? = null
  private var activityOptionsMap: MutableMap<String, ActivityOptions>? = HashMap()

  constructor(
    context: ReplayWorkflowContext,
    converter: DataConverter,
    contextPropagators: List<ContextPropagator>?,
    lastCompletionResult: Optional<Payloads?>?,
    lastFailure: Optional<Failure?>?
  ) : this(context, null, converter, contextPropagators, lastCompletionResult, lastFailure) {
  }

  // This is needed for unit tests that create DeterministicRunner directly.
  val workflowInterceptor: WorkflowOutboundCallsInterceptor
    get() =// This is needed for unit tests that create DeterministicRunner directly.
      if (headInterceptor == null) this else headInterceptor!!

  fun setHeadInterceptor(head: WorkflowOutboundCallsInterceptor?) {
    if (headInterceptor == null) {
      runner.setInterceptorHead(head)
      headInterceptor = head
    }
  }

  fun getDefaultActivityOptions(): ActivityOptions? {
    return defaultActivityOptions
  }

  val activityOptions: Map<String, ActivityOptions>?
    get() = activityOptionsMap

  fun setDefaultActivityOptions(defaultActivityOptions: ActivityOptions?) {
    this.defaultActivityOptions = if (this.defaultActivityOptions == null) defaultActivityOptions else this.defaultActivityOptions!!
      .toBuilder()
      .mergeActivityOptions(defaultActivityOptions)
      .build()
  }

  fun setActivityOptions(activityMethodOptions: Map<String, ActivityOptions>) {
    Objects.requireNonNull(activityMethodOptions)
    if (activityOptionsMap == null) {
      activityOptionsMap = HashMap(activityMethodOptions)
      return
    }
    activityMethodOptions.forEach { (key: String, value: ActivityOptions) ->
      activityOptionsMap!!.merge(
        key, value
      ) { o1: ActivityOptions, o2: ActivityOptions? -> o1.toBuilder().mergeActivityOptions(o2).build() }
    }
  }

  override fun <T> executeActivity(input: WorkflowOutboundCallsInterceptor.ActivityInput<T>): WorkflowOutboundCallsInterceptor.ActivityOutput<T> {
    val args = dataConverter.toPayloads(*input.args)
    val binaryResult = executeActivityOnce(input.activityName, input.options, input.header, args)
    return if (input.resultType === Void.TYPE) {
      WorkflowOutboundCallsInterceptor.ActivityOutput(binaryResult.thenApply { r: Optional<Payloads>? -> null })
    } else WorkflowOutboundCallsInterceptor.ActivityOutput(
      binaryResult.thenApply { r: Optional<Payloads>? -> dataConverter.fromPayloads(0, r, input.resultClass, input.resultType) })
  }

  private fun executeActivityOnce(
    name: String, options: ActivityOptions, header: Header, input: Optional<Payloads>
  ): Promise<Optional<Payloads>> {
    val callback: ActivityCallback = ActivityCallback()
    val params = constructExecuteActivityParameters(name, options, header, input)
    val cancellationCallback = context.scheduleActivityTask(params) { output: Optional<Payloads?>?, failure: Failure? -> callback.invoke(output, failure) }
    CancellationScope.current()
      .cancellationRequest
      .thenApply<Any?> { reason: String? ->
        cancellationCallback.apply(CanceledFailure(reason))
        null
      }
    return callback.result
  }

  fun handleInterceptedSignal(input: SignalInput?) {
    signalDispatcher.handleInterceptedSignal(input)
  }

  fun handleSignal(signalName: String?, input: Optional<Payloads?>?, eventId: Long) {
    signalDispatcher.handleSignal(signalName, input, eventId)
  }

  fun handleInterceptedQuery(
    input: WorkflowInboundCallsInterceptor.QueryInput?
  ): WorkflowInboundCallsInterceptor.QueryOutput {
    return queryDispatcher.handleInterceptedQuery(input)
  }

  fun handleQuery(queryName: String?, input: Optional<Payloads?>?): Optional<Payloads> {
    return queryDispatcher.handleQuery(queryName, input)
  }

  fun setHeadInboundCallsInterceptor(inbound: WorkflowInboundCallsInterceptor?) {
    signalDispatcher.setInboundCallsInterceptor(inbound)
    queryDispatcher.setInboundCallsInterceptor(inbound)
  }

  private inner class ActivityCallback {
    val result: CompletablePromise<Optional<Payloads>> = Workflow.newPromise()
    operator fun invoke(output: Optional<Payloads?>?, failure: Failure?) {
      if (failure != null) {
        runner.executeInWorkflowThread(
          "activity failure callback"
        ) {
          result.completeExceptionally(
            FailureConverter.failureToException(failure, dataConverter) as RuntimeException
          )
        }
      } else {
        runner.executeInWorkflowThread(
          "activity completion callback"
        ) { result.complete(output) }
      }
    }
  }

  override fun <R> executeLocalActivity(input: LocalActivityInput<R>): LocalActivityOutput<R> {
    val startTime = KWorkflow.currentTimeMillis()
    return LocalActivityOutput(
      WorkflowRetryerInternal.retryAsync(
        { attempt, currentStart -> executeLocalActivityOnce(input, currentStart - startTime, attempt) },
        1,
        startTime
      )
    )
  }

  private fun <T> executeLocalActivityOnce(
    input: LocalActivityInput<T>, elapsed: Long, attempt: Int
  ): Promise<T> {
    val payloads = dataConverter.toPayloads(*input.args)
    val binaryResult = executeLocalActivityOnce(
      input.activityName, input.options, input.header, payloads, attempt
    )
    return if (input.resultClass == Void.TYPE) {
      binaryResult.thenApply { r: Optional<Payloads>? -> null }
    } else binaryResult.thenApply { r: Optional<Payloads>? -> dataConverter.fromPayloads(0, r, input.resultClass, input.resultType) }
  }

  private fun executeLocalActivityOnce(
    name: String,
    options: LocalActivityOptions,
    header: Header,
    input: Optional<Payloads>,
    attempt: Int
  ): Promise<Optional<Payloads>> {
    val callback: ActivityCallback = ActivityCallback()
    val params = constructExecuteLocalActivityParameters(name, options, header, input, attempt)
    val cancellationCallback = context.scheduleLocalActivityTask(params) { output: Optional<Payloads?>?, failure: Failure? -> callback.invoke(output, failure) }
    CancellationScope.current()
      .cancellationRequest
      .thenApply<Any?> { reason: String? ->
        cancellationCallback.apply()
        null
      }
    return callback.result
  }

  private fun constructExecuteActivityParameters(
    name: String, options: ActivityOptions, header: Header, input: Optional<Payloads>
  ): ExecuteActivityParameters {
    var taskQueue = options.taskQueue
    if (taskQueue == null) {
      taskQueue = context.taskQueue
    }
    val attributes = ScheduleActivityTaskCommandAttributes.newBuilder()
      .setActivityType(ActivityType.newBuilder().setName(name))
      .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue))
      .setScheduleToStartTimeout(
        ProtobufTimeUtils.toProtoDuration(options.scheduleToStartTimeout)
      )
      .setStartToCloseTimeout(
        ProtobufTimeUtils.toProtoDuration(options.startToCloseTimeout)
      )
      .setScheduleToCloseTimeout(
        ProtobufTimeUtils.toProtoDuration(options.scheduleToCloseTimeout)
      )
      .setHeartbeatTimeout(ProtobufTimeUtils.toProtoDuration(options.heartbeatTimeout))
    if (input.isPresent) {
      attributes.input = input.get()
    }
    val retryOptions = options.retryOptions
    if (retryOptions != null) {
      attributes.setRetryPolicy(SerializerUtils.toRetryPolicy(retryOptions))
    }

    // Set the context value.  Use the context propagators from the ActivityOptions
    // if present, otherwise use the ones configured on the WorkflowContext
    var propagators = options.contextPropagators
    if (propagators == null) {
      propagators = contextPropagators
    }
    val grpcHeader = HeaderUtils.toHeaderGrpc(header, extractContextsAndConvertToBytes(propagators))
    if (grpcHeader != null) {
      attributes.header = grpcHeader
    }
    return ExecuteActivityParameters(attributes, options.cancellationType)
  }

  private fun constructExecuteLocalActivityParameters(
    name: String,
    options: LocalActivityOptions,
    header: Header,
    input: Optional<Payloads>,
    attempt: Int
  ): ExecuteLocalActivityParameters {
    var options = options
    options = LocalActivityOptions.newBuilder(options).validateAndBuildWithDefaults()
    val activityTask = PollActivityTaskQueueResponse.newBuilder()
      .setActivityId(context.randomUUID().toString())
      .setWorkflowNamespace(context.namespace)
      .setWorkflowExecution(context.workflowExecution)
      .setScheduledTime(ProtobufTimeUtils.getCurrentProtoTime())
      .setStartToCloseTimeout(
        ProtobufTimeUtils.toProtoDuration(options.startToCloseTimeout)
      )
      .setScheduleToCloseTimeout(
        ProtobufTimeUtils.toProtoDuration(options.scheduleToCloseTimeout)
      )
      .setStartedTime(ProtobufTimeUtils.getCurrentProtoTime())
      .setActivityType(ActivityType.newBuilder().setName(name))
      .setAttempt(attempt)
    val grpcHeader = HeaderUtils.toHeaderGrpc(header, extractContextsAndConvertToBytes(contextPropagators))
    if (grpcHeader != null) {
      activityTask.header = grpcHeader
    }
    if (input.isPresent) {
      activityTask.input = input.get()
    }
    val retryOptions = options.retryOptions
    activityTask.setRetryPolicy(
      SerializerUtils.toRetryPolicy(RetryOptions.newBuilder(retryOptions).validateBuildWithDefaults())
    )
    var localRetryThreshold = options.localRetryThreshold
    if (localRetryThreshold == null) {
      localRetryThreshold = context.workflowTaskTimeout.multipliedBy(6)
    }
    return ExecuteLocalActivityParameters(
      activityTask, localRetryThreshold, options.isDoNotIncludeArgumentsIntoMarker
    )
  }

  override fun <R> executeChildWorkflow(input: ChildWorkflowInput<R>): ChildWorkflowOutput<R> {
    val payloads = dataConverter.toPayloads(*input.args)
    val execution: CompletablePromise<WorkflowExecution> = Workflow.newPromise()
    val output = executeChildWorkflow(
      input.workflowId,
      input.workflowType,
      input.options,
      input.header,
      payloads,
      execution
    )
    val result = output.thenApply { b: Optional<Payloads>? -> dataConverter.fromPayloads(0, b, input.resultClass, input.resultType) }
    return ChildWorkflowOutput(result, execution)
  }

  private fun executeChildWorkflow(
    workflowId: String,
    name: String,
    options: ChildWorkflowOptions,
    header: Header,
    input: Optional<Payloads>,
    executionResult: CompletablePromise<WorkflowExecution>
  ): Promise<Optional<Payloads>> {
    val result: CompletablePromise<Optional<Payloads>> = Workflow.newPromise()
    if (CancellationScope.current().isCancelRequested) {
      val CanceledFailure = CanceledFailure("execute called from a canceled scope")
      executionResult.completeExceptionally(CanceledFailure)
      result.completeExceptionally(CanceledFailure)
      return result
    }
    var propagators = options.contextPropagators
    if (propagators == null) {
      propagators = contextPropagators
    }
    val attributes = StartChildWorkflowExecutionCommandAttributes.newBuilder()
      .setWorkflowType(WorkflowType.newBuilder().setName(name).build())
    attributes.workflowId = workflowId
    attributes.namespace = OptionsUtils.safeGet(options.namespace)
    if (input.isPresent) {
      attributes.input = input.get()
    }
    attributes.workflowRunTimeout = ProtobufTimeUtils.toProtoDuration(options.workflowRunTimeout)
    attributes.workflowExecutionTimeout = ProtobufTimeUtils.toProtoDuration(options.workflowExecutionTimeout)
    attributes.workflowTaskTimeout = ProtobufTimeUtils.toProtoDuration(options.workflowTaskTimeout)
    val taskQueue = options.taskQueue
    val tl = TaskQueue.newBuilder()
    if (taskQueue != null) {
      attributes.setTaskQueue(TaskQueue.newBuilder().setName(taskQueue))
    }
    if (options.workflowIdReusePolicy != null) {
      attributes.workflowIdReusePolicy = options.workflowIdReusePolicy
    }
    val retryOptions = options.retryOptions
    if (retryOptions != null) {
      attributes.setRetryPolicy(SerializerUtils.toRetryPolicy(retryOptions))
    }
    attributes.cronSchedule = OptionsUtils.safeGet(options.cronSchedule)
    val searchAttributes = options.searchAttributes
    if (searchAttributes != null) {
      attributes.searchAttributes = InternalUtils.convertMapToSearchAttributes(searchAttributes)
    }
    val memo = options.memo
    if (memo != null) {
      attributes.setMemo(Memo.newBuilder().putAllFields(HeaderUtils.intoPayloadMapWithDefaultConverter(memo)))
    }
    val grpcHeader = HeaderUtils.toHeaderGrpc(header, extractContextsAndConvertToBytes(propagators))
    attributes.header = grpcHeader
    val parentClosePolicy = options.parentClosePolicy
    if (parentClosePolicy != null) {
      attributes.parentClosePolicy = parentClosePolicy
    }
    val parameters = StartChildWorkflowExecutionParameters(attributes, options.cancellationType)
    val cancellationCallback = context.startChildWorkflow(
      parameters,
      { we: WorkflowExecution? ->
        runner.executeInWorkflowThread(
          "child workflow started callback"
        ) { executionResult.complete(we) }
      }
    ) { output: Optional<Payloads?>?, failure: Exception? ->
      if (failure != null) {
        runner.executeInWorkflowThread(
          "child workflow failure callback"
        ) { result.completeExceptionally(mapChildWorkflowException(failure)) }
      } else {
        runner.executeInWorkflowThread(
          "child workflow completion callback"
        ) { result.complete(output) }
      }
    }
    val callbackCalled = AtomicBoolean()
    CancellationScope.current()
      .cancellationRequest
      .thenApply<Any?> { reason: String? ->
        if (!callbackCalled.getAndSet(true)) {
          cancellationCallback.apply(CanceledFailure(reason))
        }
        null
      }
    return result
  }

  private fun extractContextsAndConvertToBytes(contextPropagators: List<ContextPropagator>?): Header? {
    if (contextPropagators == null) {
      return null
    }
    val result: MutableMap<String, Payload> = HashMap()
    for (propagator in contextPropagators) {
      result.putAll(propagator.serializeContext(propagator.currentContext))
    }
    return Header(result)
  }

  private fun mapChildWorkflowException(failure: Exception?): RuntimeException? {
    if (failure == null) {
      return null
    }
    if (failure is TemporalFailure) {
      failure.setDataConverter(dataConverter)
    }
    if (failure is CanceledFailure) {
      return failure
    }
    if (failure is WorkflowException) {
      return failure
    }
    if (failure is ChildWorkflowFailure) {
      return failure
    }
    if (failure !is ChildWorkflowTaskFailedException) {
      return IllegalArgumentException("Unexpected exception type: ", failure)
    }
    val taskFailed = failure
    var cause: Throwable? = FailureConverter.failureToException(taskFailed.failure, dataConverter)
    // To support WorkflowExecutionAlreadyStarted set at handleStartChildWorkflowExecutionFailed
    if (cause == null) {
      cause = failure.cause
    }
    return ChildWorkflowFailure(
      0,
      0,
      taskFailed.workflowType.name,
      taskFailed.workflowExecution,
      null,
      taskFailed.retryState,
      cause
    )
  }

  override fun newTimer(delay: Duration): Promise<Void> {
    val p: CompletablePromise<Void> = Workflow.newPromise()
    val cancellationHandler = context.newTimer(
      delay
    ) { e: RuntimeException? ->
      runner.executeInWorkflowThread(
        "timer-callback"
      ) {
        if (e == null) {
          p.complete(null)
        } else {
          p.completeExceptionally(e)
        }
      }
    }
    CancellationScope.current()
      .cancellationRequest
      .thenApply { r: String? ->
        cancellationHandler.apply(CanceledFailure(r))
        r
      }
    return p
  }

  override fun <R> sideEffect(resultClass: Class<R>, resultType: Type, func: Func<R>): R {
    return try {
      val dataConverter = dataConverter
      val result: CompletablePromise<Optional<Payloads>> = Workflow.newPromise()
      context.sideEffect(
        {
          val r = func.apply()
          dataConverter.toPayloads(r)
        }
      ) { p: Optional<Payloads?>? ->
        runner.executeInWorkflowThread(
          "side-effect-callback"
        ) { result.complete(Objects.requireNonNull(p)) }
      }
      dataConverter.fromPayloads(0, result.get(), resultClass, resultType)
    } catch (e: Error) {
      throw e
    } catch (e: Exception) {
      // SideEffect cannot throw normal exception as it can lead to non deterministic behavior
      // So fail the workflow task by throwing an Error.
      throw Error(e)
    }
  }

  override fun <R> mutableSideEffect(
    id: String, resultClass: Class<R>, resultType: Type, updated: BiPredicate<R, R>, func: Func<R>
  ): R {
    return try {
      mutableSideEffectImpl(id, resultClass, resultType, updated, func)
    } catch (e: Error) {
      throw e
    } catch (e: Exception) {
      // MutableSideEffect cannot throw normal exception as it can lead to non deterministic
      // behavior
      // So fail the workflow task by throwing an Error.
      throw Error(e)
    }
  }

  private fun <R> mutableSideEffectImpl(
    id: String, resultClass: Class<R>, resultType: Type, updated: BiPredicate<R, R>, func: Func<R>
  ): R {
    val result: CompletablePromise<Optional<Payloads>> = Workflow.newPromise()
    val unserializedResult = AtomicReference<R>()
    context.mutableSideEffect(
      id,
      { storedBinary: Optional<Payloads?> ->
        val stored = storedBinary.map { b: Payloads? -> dataConverter.fromPayloads(0, Optional.of(b), resultClass, resultType) }
        val funcResult = Objects.requireNonNull(func.apply(), "mutableSideEffect function " + "returned null")
        if (!stored.isPresent || updated.test(stored.get(), funcResult)) {
          unserializedResult.set(funcResult)
          return@mutableSideEffect dataConverter.toPayloads(funcResult)
        }
        Optional.empty() // returned only when value doesn't need to be updated
      }
    ) { p: Optional<Payloads?>? ->
      runner.executeInWorkflowThread(
        "mutable-side-effect-callback"
      ) { result.complete(Objects.requireNonNull(p)) }
    }
    require(result.get().isPresent()) { "No value found for mutableSideEffectId=$id" }
    // An optimization that avoids unnecessary deserialization of the result.
    val unserialized = unserializedResult.get()
    return unserialized ?: dataConverter.fromPayloads(0, result.get(), resultClass, resultType)
  }

  override fun getVersion(changeId: String, minSupported: Int, maxSupported: Int): Int {
    val result: CompletablePromise<Int> = Workflow.newPromise()
    context.getVersion(
      changeId,
      minSupported,
      maxSupported
    ) { v: Int? -> runner.executeInWorkflowThread("version-callback") { result.complete(v) } }
    return result.get()
  }

  override fun registerQuery(request: RegisterQueryInput) {
    queryDispatcher.registerQueryHandlers(request)
  }

  override fun registerSignalHandlers(input: RegisterSignalHandlersInput) {
    signalDispatcher.registerSignalHandlers(input)
  }

  override fun registerDynamicSignalHandler(input: RegisterDynamicSignalHandlerInput) {
    signalDispatcher.registerDynamicSignalHandler(input)
  }

  override fun registerDynamicQueryHandler(input: RegisterDynamicQueryHandlerInput) {
    queryDispatcher.registerDynamicQueryHandler(input)
  }

  override fun randomUUID(): UUID {
    return context.randomUUID()
  }

  override fun newRandom(): Random {
    return context.newRandom()
  }

  val isReplaying: Boolean
    get() = context.isReplaying

  override fun signalExternalWorkflow(input: SignalExternalInput): SignalExternalOutput {
    val attributes = SignalExternalWorkflowExecutionCommandAttributes.newBuilder()
    attributes.signalName = input.signalName
    attributes.execution = input.execution
    val payloads = dataConverter.toPayloads(*input.args)
    if (payloads.isPresent) {
      attributes.input = payloads.get()
    }
    val result: CompletablePromise<Void> = Workflow.newPromise()
    val cancellationCallback = context.signalExternalWorkflowExecution(
      attributes
    ) { output: Void?, failure: Failure? ->
      if (failure != null) {
        runner.executeInWorkflowThread(
          "child workflow failure callback"
        ) {
          result.completeExceptionally(
            FailureConverter.failureToException(failure, dataConverter)
          )
        }
      } else {
        runner.executeInWorkflowThread(
          "child workflow completion callback"
        ) { result.complete(output) }
      }
    }
    CancellationScope.current()
      .cancellationRequest
      .thenApply<Any?> { reason: String? ->
        cancellationCallback.apply(CanceledFailure(reason))
        null
      }
    return SignalExternalOutput(result)
  }

  override fun sleep(duration: Duration) {
    newTimer(duration).get()
  }

  override fun await(timeout: Duration, reason: String, unblockCondition: Supplier<Boolean>): Boolean {
    val timer = newTimer(timeout)
    WorkflowThread.await(reason) { timer.isCompleted || unblockCondition.get() }
    return !timer.isCompleted
  }

  override fun await(reason: String, unblockCondition: Supplier<Boolean>) {
    WorkflowThread.await(reason, unblockCondition)
  }

  override fun continueAsNew(input: ContinueAsNewInput) {
    val attributes = ContinueAsNewWorkflowExecutionCommandAttributes.newBuilder()
    val workflowType = input.workflowType
    if (workflowType.isPresent) {
      attributes.setWorkflowType(WorkflowType.newBuilder().setName(workflowType.get()))
    }
    val options = input.options
    if (options.isPresent) {
      val ops = options.get()
      attributes.workflowRunTimeout = ProtobufTimeUtils.toProtoDuration(ops.workflowRunTimeout)
      attributes.workflowTaskTimeout = ProtobufTimeUtils.toProtoDuration(ops.workflowTaskTimeout)
      if (!ops.taskQueue.isEmpty()) {
        attributes.setTaskQueue(TaskQueue.newBuilder().setName(ops.taskQueue))
      }
      val memo = ops.memo
      if (memo != null) {
        attributes.setMemo(
          Memo.newBuilder().putAllFields(HeaderUtils.intoPayloadMapWithDefaultConverter(memo))
        )
      }
      val searchAttributes = ops.searchAttributes
      if (searchAttributes != null) {
        attributes.setSearchAttributes(
          SearchAttributes.newBuilder()
            .putAllIndexedFields(HeaderUtils.intoPayloadMapWithDefaultConverter(searchAttributes))
        )
      }
    }
    val payloads = dataConverter.toPayloads(*input.args)
    if (payloads.isPresent) {
      attributes.input = payloads.get()
    }
    // TODO(maxim): Find out what to do about header
    context.continueAsNewOnCompletion(attributes.build())
    WorkflowThread.exit(null)
  }

  override fun cancelWorkflow(input: CancelWorkflowInput): CancelWorkflowOutput {
    val result: CompletablePromise<Void> = Workflow.newPromise()
    context.requestCancelExternalWorkflowExecution(
      input.execution
    ) { r: Void?, exception: RuntimeException? ->
      if (exception == null) {
        result.complete(null)
      } else {
        result.completeExceptionally(exception)
      }
    }
    return CancelWorkflowOutput(result)
  }

  val metricsScope: Scope
    get() = context.metricsScope
  val isLoggingEnabledInReplay: Boolean
    get() = context.enableLoggingInReplay

  fun <R> getLastCompletionResult(resultClass: Class<R>?, resultType: Type?): R {
    val dataConverter = dataConverter
    return dataConverter.fromPayloads(0, lastCompletionResult, resultClass, resultType)
  }

  override fun upsertSearchAttributes(searchAttributes: Map<String, Any>) {
    require(!searchAttributes.isEmpty()) { "Empty search attributes" }
    val attr = InternalUtils.convertMapToSearchAttributes(searchAttributes)
    context.upsertSearchAttributes(attr)
  }

  override fun newThread(runnable: Runnable, detached: Boolean, name: String): Any {
    throw NotImplementedException()
  }

  override fun currentTimeMillis(): Long {
    return context.currentTimeMillis()
  }

  init {
    signalDispatcher = SignalDispatcher(dataConverter)
    queryDispatcher = QueryDispatcher(dataConverter)
    if (workflowImplementationOptions != null) {
      defaultActivityOptions = workflowImplementationOptions.defaultActivityOptions
      activityOptionsMap = workflowImplementationOptions.activityOptions
    }
  }
}