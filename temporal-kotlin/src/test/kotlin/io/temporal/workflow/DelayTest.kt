package io.temporal.workflow

import io.temporal.worker.CoroutineReplayWorkflow
import io.temporal.worker.KWorkflow
import io.temporal.workflow.shared.SDKTestWorkflowRule
import io.temporal.workflow.shared.TestWorkflows.TestWorkflow1
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import java.lang.StringBuilder
import java.time.Instant

class DelayTest {

  @get:Rule
  val testWorkflowRule = SDKTestWorkflowRule.newBuilder()
    .setWorkflowTypes(TestDelay::class.java)
    .setSyncReplayWorkflowFactory(CoroutineReplayWorkflow.Factory())
    .build()

  @Test fun testAwait() {
    val workflowStub: TestWorkflow1 =
      testWorkflowRule.newWorkflowStubTimeoutOptions<TestWorkflow1>(TestWorkflow1::class.java)
    val result: String = workflowStub.execute(testWorkflowRule.getTaskQueue())
    assertEquals(" awoken i=1 loop i=1 awoken i=2 loop i=2 awoken i=3", result)
  }

  class TestDelay : TestWorkflow1 {

    override fun execute(taskQueue: String) = runBlocking {
      val result = StringBuilder()
      var i = 0
      println("i=$i,${Instant.ofEpochMilli(KWorkflow.currentTimeMillis())},${Instant.ofEpochMilli(System.currentTimeMillis())}")
      while (i < 5) {
        delay(1000)
        i += 1
        println("i=$i,${Instant.ofEpochMilli(KWorkflow.currentTimeMillis())},${Instant.ofEpochMilli(System.currentTimeMillis())}")
        result.append("i=$i,${KWorkflow.currentTimeMillis()},${System.currentTimeMillis()}")
      }
      result.toString()
    }
  }
}