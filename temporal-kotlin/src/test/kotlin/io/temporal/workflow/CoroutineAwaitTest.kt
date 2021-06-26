package io.temporal.workflow

import io.temporal.workflow.shared.SDKTestWorkflowRule
import io.temporal.workflow.shared.TestWorkflows.TestWorkflow1
import org.junit.Assert.assertFalse
import org.junit.Rule
import org.junit.Test
import java.time.Duration

class CoroutineAwaitTest {

  @get:Rule
  val testWorkflowRule = SDKTestWorkflowRule.newBuilder().setWorkflowTypes(TestAwait::class.java).build()

  @Test fun testAwait() {
    val workflowStub: TestWorkflow1 =
      testWorkflowRule.newWorkflowStubTimeoutOptions<TestWorkflow1>(TestWorkflow1::class.java)
    val result: String = workflowStub.execute(testWorkflowRule.getTaskQueue())
    org.junit.Assert.assertEquals(" awoken i=1 loop i=1 awoken i=2 loop i=2 awoken i=3", result)
  }

  class TestAwait : TestWorkflow1 {
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
      assertFalse(Workflow.await(Duration.ZERO) { false })
      return result.toString()
    }
  }
}