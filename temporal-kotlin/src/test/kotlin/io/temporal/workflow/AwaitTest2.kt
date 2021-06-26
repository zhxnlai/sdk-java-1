package io.temporal.workflow

import io.temporal.workflow.shared.SDKTestWorkflowRule
import io.temporal.workflow.shared.TestWorkflows.TestWorkflow1
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertFalse
import org.junit.Rule
import org.junit.Test
import java.time.Duration

class AwaitTest2 {

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
    override fun execute(taskQueue: String) = runBlocking {
      val result = StringBuilder()
      launch {
        while (true) {
          if (i <= j) {
            delay(100)
            continue
          }
          result.append(" awoken i=$i")
          println(" awoken i=$i, j=$j")
          j++
        }
      }
      i = 1
      while (i < 3) {
        if (j < i) {
          delay(100)
          continue
        }
        result.append(" loop i=$i")
        println(" loop i=$i, j=$j")
        i++
      }
      // assertFalse(Workflow.await(Duration.ZERO) { false })
      result.toString()
    }
  }
}