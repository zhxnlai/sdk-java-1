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

import io.temporal.common.interceptors.Header
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor

/**
 * Intercepts calls to the workflow execution. Executes under workflow context. So all the
 * restrictions on the workflow code should be obeyed.
 */
interface WorkflowInboundCallsInterceptor {
  class WorkflowInput(val header: Header, val arguments: Array<Any>)
  class WorkflowOutput(val result: Any)
  class SignalInput(val signalName: String, val arguments: Array<Any>, val eventId: Long)
  class QueryInput(val queryName: String, val arguments: Array<Any>)
  class QueryOutput(val result: Any)

  /**
   * Called when workflow class is instantiated.
   *
   * @param outboundCalls interceptor for calls that workflow makes to the SDK
   */
  fun init(outboundCalls: WorkflowOutboundCallsInterceptor?)

  /**
   * Called when workflow main method is called.
   *
   * @return result of the workflow execution.
   */
  fun execute(input: WorkflowInput?): WorkflowOutput?

  /** Called when signal is delivered to a workflow execution.  */
  fun handleSignal(input: SignalInput?)

  /** Called when a workflow is queried.  */
  fun handleQuery(input: QueryInput?): QueryOutput?
}