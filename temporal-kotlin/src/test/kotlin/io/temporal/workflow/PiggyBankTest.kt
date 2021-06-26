package io.temporal.workflow

import io.temporal.workflow.shared.SDKTestWorkflowRule
import io.temporal.workflow.shared.TestWorkflows.TestWorkflow1
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import java.time.Duration

class PiggyBankTest {

  @get:Rule
  val testWorkflowRule = SDKTestWorkflowRule.newBuilder().setWorkflowTypes(PiggyBankWorkflowImpl::class.java).build()

  @Test fun testAwait() {
    val workflowStub: PiggyBankWorkflow =
      testWorkflowRule.newWorkflowStubTimeoutOptions<PiggyBankWorkflow>(PiggyBankWorkflow::class.java)
    workflowStub.saveUntil(1)
    testWorkflowRule.sleep(Duration.ofSeconds(1))
  }

  @WorkflowInterface
  interface PiggyBankWorkflow {
    @WorkflowMethod
    fun saveUntil(targetAmountCents: Int)

    // TODO should this be suspending? or a send channel
    @SignalMethod
    fun deposit(amountCents: Int)

    @QueryMethod
    fun balance(): Int
  }

  class PiggyBankWorkflowImpl : PiggyBankWorkflow {

    private var balance = 0
    private val depositCents = Channel<Int>()

    override fun saveUntil(targetAmountCents: Int) = runBlocking {
      for (depositCent in depositCents) {
        balance += depositCent
        if (balance >= targetAmountCents) {
          break
        }
      }
      println("It's time")
    }

    override fun deposit(amountCents: Int) {
      runBlocking { depositCents.send(amountCents) }
    }

    override fun balance(): Int {
      return balance
    }
  }

}