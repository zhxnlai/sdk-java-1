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
package io.temporal.api

import io.temporal.activity.ActivityOptions
import io.temporal.activity.LocalActivityOptions
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.common.interceptors.Header
import java.lang.Void
import io.temporal.workflow.ContinueAsNewOptions
import io.temporal.workflow.Functions.Proc1
import io.temporal.workflow.Functions.Func1
import io.temporal.workflow.DynamicQueryHandler
import io.temporal.workflow.DynamicSignalHandler
import io.temporal.workflow.Functions.Func
import io.temporal.workflow.Promise
import java.util.function.BiPredicate
import java.util.UUID
import java.lang.reflect.Type
import java.util.Optional

/**
 * Can be used to intercept workflow code calls to the Temporal APIs. An instance should be created
 * through [WorkerInterceptor.interceptWorkflow]. An
 * interceptor instance must forward all the calls to the next interceptor passed to the
 * interceptExecuteWorkflow call.
 *
 *
 * The calls to the interceptor are executed in the context of a workflow and must follow the
 * same rules all the other workflow code follows.
 */
interface WorkflowOutboundCallsInterceptor {
  class ActivityInput<R>(
    val activityName: String,
    val resultClass: Class<R>,
    val resultType: Type,
    val args: Array<Any>,
    val options: ActivityOptions,
    val header: Header
  )

  class ActivityOutput<R>(val result: Promise<R>)
  class LocalActivityInput<R>(
    val activityName: String,
    val resultClass: Class<R>,
    val resultType: Type,
    val args: Array<Any>,
    val options: LocalActivityOptions,
    val header: Header
  )

  class LocalActivityOutput<R>(val result: Promise<R>)
  class ChildWorkflowInput<R>(
    val workflowId: String,
    val workflowType: String,
    val resultClass: Class<R>,
    val resultType: Type,
    val args: Array<Any>,
    val options: ChildWorkflowOptions,
    val header: Header
  )

  class ChildWorkflowOutput<R>(val result: Promise<R>, val workflowExecution: Promise<WorkflowExecution>)
  class SignalExternalInput(val execution: WorkflowExecution, val signalName: String, val args: Array<Any>)
  class SignalExternalOutput(val result: Promise<Void>)
  class CancelWorkflowInput(val execution: WorkflowExecution)
  class CancelWorkflowOutput(val result: Promise<Void>)
  class ContinueAsNewInput(
    val workflowType: Optional<String>,
    val options: Optional<ContinueAsNewOptions>,
    val args: Array<Any>,
    val header: Header
  )

  class SignalRegistrationRequest(
    val signalType: String,
    val argTypes: Array<Class<*>>,
    val genericArgTypes: Array<Type>,
    val callback: Proc1<Array<Any>>
  )

  class RegisterSignalHandlersInput(val requests: List<SignalRegistrationRequest>)
  class RegisterQueryInput(
    val queryType: String,
    val argTypes: Array<Class<*>>,
    val genericArgTypes: Array<Type>,
    val callback: Func1<Array<Any>, Any>
  )

  class RegisterDynamicQueryHandlerInput(val handler: DynamicQueryHandler)
  class RegisterDynamicSignalHandlerInput(val handler: DynamicSignalHandler)

  fun <R> executeActivity(input: ActivityInput<R>?): ActivityOutput<R>?
  fun <R> executeLocalActivity(input: LocalActivityInput<R>?): LocalActivityOutput<R>?
  fun <R> executeChildWorkflow(input: ChildWorkflowInput<R>?): ChildWorkflowOutput<R>?
  fun signalExternalWorkflow(input: SignalExternalInput?): SignalExternalOutput?
  fun cancelWorkflow(input: CancelWorkflowInput?): CancelWorkflowOutput?
  fun <R> sideEffect(resultClass: Class<R>?, resultType: Type?, func: Func<R>?): R
  fun <R> mutableSideEffect(
    id: String?, resultClass: Class<R>?, resultType: Type?, updated: BiPredicate<R, R>?, func: Func<R>?
  ): R

  fun getVersion(changeId: String?, minSupported: Int, maxSupported: Int): Int
  fun continueAsNew(input: ContinueAsNewInput?)
  fun registerQuery(input: RegisterQueryInput?)
  fun registerSignalHandlers(input: RegisterSignalHandlersInput?)
  fun registerDynamicSignalHandler(handler: RegisterDynamicSignalHandlerInput?)
  fun registerDynamicQueryHandler(input: RegisterDynamicQueryHandlerInput?)
  fun randomUUID(): UUID?
  fun upsertSearchAttributes(searchAttributes: Map<String?, Any?>?)
  fun currentTimeMillis(): Long
}