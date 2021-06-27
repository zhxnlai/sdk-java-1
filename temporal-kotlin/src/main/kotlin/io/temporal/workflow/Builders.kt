package io.temporal.workflow

import io.temporal.worker.WorkflowCoroutineScope
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.reflect.KClass

data class WorkflowOptions(
  val workflowId: String
  // task queue
  // ....
)

interface ClientCoroutineScope : CoroutineScope {

}

class WorkflowClient() {

  fun <R> execute(workflowId: String, body: suspend ClientCoroutineScope.() -> R) {
  }

  suspend fun <T : Any> query(workflowId: String, runId: String = "", name: String, dataType: KClass<T>): T {
    TODO()
  }

  suspend fun <T : Any> signal(workflowId: String, runId: String = "", name: String, dataType: KClass<T>, data: T) {
    TODO()
  }

  suspend inline fun <reified T : Any> query(workflowId: String, runId: String = "", name: String): T {
    return query(workflowId, runId, name, T::class)
  }

  suspend inline fun <reified T : Any> signal(workflowId: String, runId: String = "", name: String, data: T) {
    return signal(workflowId, runId, name, T::class, data)
  }
}

class Worker(
  private val workflowClient: WorkflowClient,
) {
  fun registerWorkflow(function: Function<*>) {
  }
}

suspend fun <P, R> workflow(p1: P, workflowBody: suspend WorkflowCoroutineScope.() -> R) {
}

suspend fun <P1, P2, R> workflow(p1: P1, p2: P2, workflowBody: suspend WorkflowCoroutineScope.() -> R) {
}

suspend fun <P1, P2, P3, R> workflow(p1: P1, p2: P2, p3: P3, workflowBody: suspend WorkflowCoroutineScope.() -> R) {
}

suspend fun <P, R> activity(p1: P, activityBody: suspend WorkflowCoroutineScope.() -> R) {
}

suspend fun <P1, P2, R> activity(p1: P1, p2: P2, activityBody: suspend WorkflowCoroutineScope.() -> R) {
}

suspend fun <P1, P2, P3, R> activity(p1: P1, p2: P2, p3: P3, activityBody: suspend WorkflowCoroutineScope.() -> R) {
}

suspend fun <T> WorkflowCoroutineScope.signalChannel(name: String): Channel<T> {
  TODO()
}

private const val PIGGY_BANK_DEPOSITS = "deposits"

suspend fun piggyBankWorkflow(targetAmountCents: Int, initialBalance: Int) = workflow(targetAmountCents, initialBalance) {
  val depositCents = signalChannel<Int>(PIGGY_BANK_DEPOSITS)

  var balance = 0
  for (depositCent in depositCents) {
    balance += depositCent
    if (balance >= targetAmountCents) {
      break
    }
  }
  sendEmailActivity("It's time")
}

private suspend fun WorkflowClient.piggyBankDeposit(workflowId: String, amount: Int) {
  signal(workflowId = workflowId, runId = "", name = PIGGY_BANK_DEPOSITS, amount)
}

private suspend fun sendEmailActivity(content: String) = activity(content) {
  println(content)
}

fun main() = runBlocking {
  lateinit var client: WorkflowClient
  val worker = Worker(client)
  worker.registerWorkflow(::piggyBankWorkflow)

  // blocks until the workflow completes...
  client.execute(workflowId = "123") {
    piggyBankWorkflow(targetAmountCents = 100_00, initialBalance = 0)
  }
  delay(1_000 * 3600)
  client.piggyBankDeposit(workflowId = "123", amount = 1_00)
  delay(1_000 * 3600)
  client.piggyBankDeposit(workflowId = "123", amount = 3_00)

}
