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

import com.google.common.base.Preconditions
import io.temporal.worker.KWorkflow.getInfo
import io.temporal.internal.worker.SingleWorkerOptions
import java.util.concurrent.ExecutorService
import io.temporal.common.interceptors.WorkerInterceptor
import io.temporal.internal.replay.WorkflowExecutorCache
import io.temporal.internal.replay.ReplayWorkflowFactory
import io.temporal.common.converter.DataConverter
import io.temporal.common.context.ContextPropagator
import io.temporal.workflow.Functions.Func
import io.temporal.internal.sync.SyncWorkflowDefinition
import java.util.Collections
import java.util.HashMap
import io.temporal.worker.WorkflowImplementationOptions
import io.temporal.workflow.DynamicWorkflow
import java.lang.IllegalStateException
import io.temporal.common.metadata.POJOWorkflowInterfaceMetadata
import java.lang.IllegalArgumentException
import io.temporal.common.metadata.POJOWorkflowMethodMetadata
import io.temporal.common.metadata.WorkflowMethodType
import java.lang.NoSuchMethodException
import java.lang.IllegalAccessException
import java.lang.reflect.InvocationTargetException
import io.temporal.common.metadata.POJOWorkflowImplMetadata
import io.temporal.api.common.v1.WorkflowType
import io.temporal.internal.sync.DynamicSyncWorkflowDefinition
import io.temporal.internal.sync.SyncReplayWorkflowFactory
import io.temporal.internal.replay.ReplayWorkflow
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor
import io.temporal.internal.sync.WorkflowInternal
import kotlin.Throws
import io.temporal.failure.CanceledFailure
import io.temporal.internal.worker.WorkflowExecutionException
import io.temporal.api.common.v1.Payloads
import io.temporal.common.interceptors.Header
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor.WorkflowOutput
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor.WorkflowInput
import io.temporal.workflow.WorkflowInfo
import io.temporal.worker.KWorkflow
import io.temporal.failure.TemporalFailure
import io.temporal.failure.FailureConverter
import io.temporal.serviceclient.CheckedExceptionWrapper
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor.SignalInput
import io.temporal.internal.sync.SyncWorkflowContext
import org.slf4j.LoggerFactory
import java.lang.Error
import java.lang.Exception
import java.lang.reflect.Method
import java.util.Objects
import java.util.Optional

class POJOWorkflowImplementationFactory internal constructor(
  singleWorkerOptions: SingleWorkerOptions,
  threadPool: ExecutorService,
  workerInterceptors: Array<WorkerInterceptor?>?,
  cache: WorkflowExecutorCache
) : ReplayWorkflowFactory {
  private val workerInterceptors: Array<WorkerInterceptor>
  private var dataConverter: DataConverter
  private val contextPropagators: List<ContextPropagator>
  private val defaultDeadlockDetectionTimeout: Long

  /** Key: workflow type name, Value: function that creates SyncWorkflowDefinition instance.  */
  private val workflowDefinitions = Collections.synchronizedMap(HashMap<String, Func<SyncWorkflowDefinition>>())
  private val implementationOptions = Collections.synchronizedMap(HashMap<String, WorkflowImplementationOptions>())
  private val workflowImplementationFactories = Collections.synchronizedMap(HashMap<Class<*>, Func<*>>())

  /** If present then it is called for any unknown workflow type.  */
  private var dynamicWorkflowImplementationFactory: Func<out DynamicWorkflow?>? = null
  private val threadPool: ExecutorService
  private val cache: WorkflowExecutorCache
  fun registerWorkflowImplementationTypes(
    options: WorkflowImplementationOptions, workflowImplementationTypes: Array<Class<*>>
  ) {
    for (type in workflowImplementationTypes) {
      registerWorkflowImplementationType(options, type)
    }
  }

  fun <R> addWorkflowImplementationFactory(clazz: Class<R>, factory: Func<R?>?) {
    val unitTestingOptions = WorkflowImplementationOptions.newBuilder()
      .setFailWorkflowExceptionTypes(Throwable::class.java)
      .build()
    addWorkflowImplementationFactory(unitTestingOptions, clazz, factory)
  }

  fun <R> addWorkflowImplementationFactory(
    options: WorkflowImplementationOptions, clazz: Class<R>, factory: Func<R?>?
  ) {
    if (DynamicWorkflow::class.java.isAssignableFrom(clazz)) {
      check(dynamicWorkflowImplementationFactory == null) { "An implementation of DynamicWorkflow or its factory is already registered with the worker" }
      dynamicWorkflowImplementationFactory = factory as Func<out DynamicWorkflow?>?
      return
    }
    workflowImplementationFactories[clazz] = factory
    val workflowMetadata = POJOWorkflowInterfaceMetadata.newInstance(clazz)
    require(workflowMetadata.workflowMethod.isPresent) { "Workflow interface doesn't contain a method annotated with @WorkflowMethod: $clazz" }
    val methodsMetadata = workflowMetadata.methodsMetadata
    for (methodMetadata in methodsMetadata) {
      when (methodMetadata.type) {
        WorkflowMethodType.WORKFLOW -> {
          val workflowName = methodMetadata.name
          check(!workflowDefinitions.containsKey(workflowName)) { "$workflowName workflow type is already registered with the worker" }
          workflowDefinitions[workflowName] = Func {
            POJOWorkflowImplementation(
              clazz, methodMetadata.name, methodMetadata.workflowMethod
            )
          }
          implementationOptions[workflowName] = options
        }
        WorkflowMethodType.SIGNAL -> {
        }
      }
    }
  }

  private fun <T> registerWorkflowImplementationType(
    options: WorkflowImplementationOptions, workflowImplementationClass: Class<T>
  ) {
    if (DynamicWorkflow::class.java.isAssignableFrom(workflowImplementationClass)) {
      addWorkflowImplementationFactory(
        options,
        workflowImplementationClass
      ) {
        try {
          return@addWorkflowImplementationFactory workflowImplementationClass.getDeclaredConstructor().newInstance()
        } catch (e: NoSuchMethodException) {
          // Error to fail workflow task as this can be fixed by a new deployment.
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        } catch (e: InstantiationException) {
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        } catch (e: IllegalAccessException) {
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        } catch (e: InvocationTargetException) {
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        }
      }
      return
    }
    var hasWorkflowMethod = false
    val workflowMetadata = POJOWorkflowImplMetadata.newInstance(workflowImplementationClass)
    for (workflowInterface in workflowMetadata.workflowInterfaces) {
      val workflowMethod = workflowInterface.workflowMethod
      if (!workflowMethod.isPresent) {
        continue
      }
      val methodMetadata = workflowMethod.get()
      val workflowName = methodMetadata.name
      val method = methodMetadata.workflowMethod
      val factory = Func<SyncWorkflowDefinition> { POJOWorkflowImplementation(workflowImplementationClass, workflowName, method) }
      check(!workflowDefinitions.containsKey(workflowName)) { "$workflowName workflow type is already registered with the worker" }
      workflowDefinitions[workflowName] = factory
      implementationOptions[workflowName] = options
      hasWorkflowMethod = true
    }
    require(hasWorkflowMethod) {
      ("Workflow implementation doesn't implement any interface "
        + "with a workflow method annotated with @WorkflowMethod: "
        + workflowImplementationClass)
    }
  }

  private fun getWorkflowDefinition(workflowType: WorkflowType): SyncWorkflowDefinition {
    val factory = workflowDefinitions[workflowType.name]
    if (factory == null) {
      if (dynamicWorkflowImplementationFactory != null) {
        return DynamicSyncWorkflowDefinition(
          dynamicWorkflowImplementationFactory, workerInterceptors, dataConverter
        )
      }
      throw Error(
        "Unknown workflow type \""
          + workflowType.name
          + "\". Known types are "
          + workflowDefinitions.keys
      )
    }
    return try {
      factory.apply()
    } catch (e: Exception) {
      throw Error(e)
    }
  }

  fun setDataConverter(dataConverter: DataConverter) {
    this.dataConverter = dataConverter
  }

  // TODO(zhixuan) implement this
  private val syncReplayWorkflowFactory: SyncReplayWorkflowFactory = CoroutineReplayWorkflow.Factory()
  override fun getWorkflow(workflowType: WorkflowType): ReplayWorkflow {
    val workflow = getWorkflowDefinition(workflowType)
    val options = implementationOptions[workflowType.name]
    return syncReplayWorkflowFactory.getWorkflow(
      workflow, options, dataConverter, contextPropagators
    )
  }

  override fun isAnyTypeSupported(): Boolean {
    return !workflowDefinitions.isEmpty() || dynamicWorkflowImplementationFactory != null
  }

  private inner class POJOWorkflowImplementation(
    private val workflowImplementationClass: Class<*>, private val workflowName: String, private val workflowMethod: Method
  ) : SyncWorkflowDefinition {
    private var workflow: Any? = null
    private var workflowInvoker: WorkflowInboundCallsInterceptor? = null
    override fun initialize() {
      val workflowContext = WorkflowInternal.getRootWorkflowContext()
      workflowInvoker = RootWorkflowInboundCallsInterceptor(workflowContext)
      for (workerInterceptor in workerInterceptors) {
        workflowInvoker = workerInterceptor.interceptWorkflow(workflowInvoker)
      }
      workflowContext.setHeadInboundCallsInterceptor(workflowInvoker)
      workflowInvoker!!.init(workflowContext)
    }

    @Throws(
      CanceledFailure::class, WorkflowExecutionException::class
    ) override fun execute(header: Header, input: Optional<Payloads>): Optional<Payloads> {
      val args = DataConverter.arrayFromPayloads(
        dataConverter,
        input,
        workflowMethod.parameterTypes,
        workflowMethod.genericParameterTypes
      )
      Preconditions.checkNotNull(workflowInvoker, "initialize not called")
      val result = workflowInvoker!!.execute(WorkflowInput(header, args))
      return if (workflowMethod.returnType == Void.TYPE) {
        Optional.empty()
      } else dataConverter.toPayloads(result.result)
    }

    private fun newInstance() {
      check(workflow == null) { "Already called" }
      val factory = workflowImplementationFactories[workflowImplementationClass]
      workflow = if (factory != null) {
        factory.apply()
      } else {
        try {
          workflowImplementationClass.getDeclaredConstructor().newInstance()
        } catch (e: NoSuchMethodException) {
          // Error to fail workflow task as this can be fixed by a new deployment.
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        } catch (e: InstantiationException) {
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        } catch (e: IllegalAccessException) {
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        } catch (e: InvocationTargetException) {
          throw Error(
            "Failure instantiating workflow implementation class "
              + workflowImplementationClass.name,
            e
          )
        }
      }
    }

    private inner class RootWorkflowInboundCallsInterceptor(private val workflowContext: SyncWorkflowContext) : WorkflowInboundCallsInterceptor {
      override fun execute(input: WorkflowInput): WorkflowOutput {
        val info = getInfo()
        return try {
          val result = workflowMethod.invoke(workflow, *input.arguments)
          WorkflowOutput(result)
        } catch (e: IllegalAccessException) {
          throw Error(mapToWorkflowExecutionException(e, dataConverter))
        } catch (e: InvocationTargetException) {
          val target = e.targetException
          // TODO
          //if (target instanceof DestroyWorkflowThreadError) {
          //  throw (DestroyWorkflowThreadError) target;
          //}
          val exception = WorkflowInternal.unwrap(target)
          val options = implementationOptions[info.workflowType]
          val failTypes = options!!.failWorkflowExceptionTypes
          if (exception is TemporalFailure) {
            logWorkflowExecutionException(info, exception)
            throw mapToWorkflowExecutionException(exception, dataConverter)
          }
          for (failType in failTypes) {
            if (failType.isAssignableFrom(exception.javaClass)) {
              // fail workflow
              if (log.isErrorEnabled) {
                val cancelRequested = WorkflowInternal.getRootWorkflowContext().context.isCancelRequested
                if (!cancelRequested || !FailureConverter.isCanceledCause(exception)) {
                  logWorkflowExecutionException(info, exception)
                }
              }
              throw mapToWorkflowExecutionException(exception, dataConverter)
            }
          }
          throw CheckedExceptionWrapper.wrap(exception)
        }
      }

      private fun logWorkflowExecutionException(info: WorkflowInfo, exception: Throwable) {
        log.error(
          "Workflow execution failure "
            + "WorkflowId="
            + info.workflowId
            + ", RunId="
            + info.runId
            + ", WorkflowType="
            + info.workflowType,
          exception
        )
      }

      override fun init(outboundCalls: WorkflowOutboundCallsInterceptor) {
        WorkflowInternal.getRootWorkflowContext().setHeadInterceptor(outboundCalls)
        newInstance()
        WorkflowInternal.registerListener(workflow)
      }

      override fun handleSignal(input: SignalInput) {
        workflowContext.handleInterceptedSignal(input)
      }

      override fun handleQuery(input: WorkflowInboundCallsInterceptor.QueryInput): WorkflowInboundCallsInterceptor.QueryOutput {
        return workflowContext.handleInterceptedQuery(input)
      }
    }
  }

  override fun toString(): String {
    return ("POJOWorkflowImplementationFactory{"
      + "registeredWorkflowTypes="
      + workflowDefinitions.keys
      + '}')
  }

  companion object {
    private val log = LoggerFactory.getLogger(POJOWorkflowImplementationFactory::class.java)
    fun mapToWorkflowExecutionException(
      exception: Throwable?, dataConverter: DataConverter?
    ): WorkflowExecutionException {
      var e = exception
      while (e != null) {
        if (e is TemporalFailure) {
          e.setDataConverter(dataConverter)
        }
        e = e.cause
      }
      val failure = FailureConverter.exceptionToFailure(exception)
      return WorkflowExecutionException(failure)
    }
  }

  init {
    Objects.requireNonNull(singleWorkerOptions)
    dataConverter = singleWorkerOptions.dataConverter
    this.threadPool = Objects.requireNonNull(threadPool)
    this.workerInterceptors = Objects.requireNonNull(workerInterceptors)
    this.cache = cache
    contextPropagators = singleWorkerOptions.contextPropagators
    defaultDeadlockDetectionTimeout = singleWorkerOptions.defaultDeadlockDetectionTimeout
  }
}