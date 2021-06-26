package io.temporal.workflow

import io.temporal.workflow.shared.SDKTestWorkflowRule
import io.temporal.workflow.shared.TestWorkflows.TestWorkflow1
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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
    org.junit.Assert.assertEquals("1491625Done!", result)
  }

  class TestAwait : TestWorkflow1 {
    private var i = 0
    private var j = 0
    override fun execute(taskQueue: String) = runBlocking {
      val result = StringBuilder()
      val channel = Channel<Int>()
      launch {
        // this might be heavy CPU-consuming computation or async logic, we'll just send five squares
        for (x in 1..5) channel.send(x * x)
      }
      // here we print five received integers:
      repeat(5) { result.append(channel.receive()) }
      result.append("Done!")
      result.toString()
    }
  }
}