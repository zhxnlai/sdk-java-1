package io.temporal.workflow

import io.temporal.workflow.Functions.Proc
import org.junit.Rule
import java.time.Duration

class AwaitTest {

  @Rule
  var testWorkflowRule = SDKTestWorkflowRule.newBuilder().setWorkflowTypes(TestAwait::class.java).build()

  @org.junit.Test fun testAwait() {
    val workflowStub: io.temporal.workflow.shared.TestWorkflows.TestWorkflow1 =
      testWorkflowRule.newWorkflowStubTimeoutOptions<io.temporal.workflow.shared.TestWorkflows.TestWorkflow1>(io.temporal.workflow.shared.TestWorkflows.TestWorkflow1::class.java)
    val result: String = workflowStub.execute(testWorkflowRule.getTaskQueue())
    org.junit.Assert.assertEquals(" awoken i=1 loop i=1 awoken i=2 loop i=2 awoken i=3", result)
  }

  class TestAwait : io.temporal.workflow.shared.TestWorkflows.TestWorkflow1 {
    private var i = 0
    private var j = 0
    override fun execute(taskQueue: String): String {
      val result = StringBuilder()
      Async.procedure {
        while (true) {
          Workflow.await { i > j }
          result.append(" awoken i=$i")
          j++
        }
      }
      i = 1
      while (i < 3) {
        Workflow.await { j >= i }
        result.append(" loop i=$i")
        i++
      }
      org.junit.Assert.assertFalse(Workflow.await(Duration.ZERO) { false })
      return result.toString()
    }
  }
}