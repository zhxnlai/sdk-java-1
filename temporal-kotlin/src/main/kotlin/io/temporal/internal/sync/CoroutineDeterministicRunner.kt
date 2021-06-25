package io.temporal.internal.sync

import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor
import io.temporal.internal.sync.DeterministicRunner

class CoroutineDeterministicRunner: DeterministicRunner {

  override fun runUntilAllBlocked(deadlockDetectionTimeout: Long) {
    TODO("Not yet implemented")
  }

  override fun isDone(): Boolean {
    TODO("Not yet implemented")
  }

  override fun getExitValue(): Any {
    TODO("Not yet implemented")
  }

  override fun cancel(reason: String?) {
    TODO("Not yet implemented")
  }

  override fun close() {
    TODO("Not yet implemented")
  }

  override fun stackTrace(): String {
    TODO("Not yet implemented")
  }

  override fun executeInWorkflowThread(name: String?, r: Runnable?) {
    TODO("Not yet implemented")
  }

  override fun newThread(runnable: Runnable?, detached: Boolean, name: String?): WorkflowThread {
    TODO("Not yet implemented")
  }

  override fun setInterceptorHead(interceptorHead: WorkflowOutboundCallsInterceptor?) {
    TODO("Not yet implemented")
  }
}