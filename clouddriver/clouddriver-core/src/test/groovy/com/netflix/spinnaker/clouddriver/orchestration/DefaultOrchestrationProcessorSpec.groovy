/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.orchestration

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.clouddriver.config.ExceptionClassifierConfigurationProperties
import com.netflix.spinnaker.clouddriver.data.task.DefaultTask
import com.netflix.spinnaker.clouddriver.data.task.DefaultTaskStatus
import com.netflix.spinnaker.clouddriver.data.task.SagaId
import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskState
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.kork.web.context.AuthenticatedRequestContextProvider
import com.netflix.spinnaker.kork.web.exceptions.ExceptionMessageDecorator
import com.netflix.spinnaker.kork.web.exceptions.ExceptionSummaryService
import com.netflix.spinnaker.security.AuthenticatedRequest
import org.slf4j.MDC
import org.springframework.beans.factory.config.AutowireCapableBeanFactory
import org.springframework.context.ApplicationContext
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

class DefaultOrchestrationProcessorSpec extends Specification {

  @Subject
  DefaultOrchestrationProcessor processor

  @Shared
  ApplicationContext applicationContext

  TaskRepository taskRepository

  DynamicConfigService dynamicConfigService

  ExceptionSummaryService exceptionSummaryService

  String taskKey
  private AuthenticatedRequestContextProvider contextProvider

  def setup() {
    taskKey = UUID.randomUUID().toString()

    taskRepository = Mock(TaskRepository)
    applicationContext = Mock(ApplicationContext)
    applicationContext.getAutowireCapableBeanFactory() >> Mock(AutowireCapableBeanFactory)
    dynamicConfigService = Mock(DynamicConfigService)
    contextProvider = new AuthenticatedRequestContextProvider()
    exceptionSummaryService = new ExceptionSummaryService(Mock(ExceptionMessageDecorator))

    processor = new DefaultOrchestrationProcessor(
      taskRepository,
      applicationContext,
      new NoopRegistry(),
      Optional.empty(),
      new ObjectMapper(),
      new ExceptionClassifier(new ExceptionClassifierConfigurationProperties(
        retryableClasses: [RetryableException.class.getName()]
      ), dynamicConfigService),
      contextProvider,
      exceptionSummaryService
    )
  }

  void "complete the task when everything goes as planned"() {
    setup:
    def task = new DefaultTask("1")
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    task.status.isCompleted()
    !task.status.isFailed()
  }

  void "single op success writes one completed history entry and no Processing op row"() {
    setup:
    def task = new DefaultTask("1")
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    task.status.isCompleted()
    task.status.phase == "ORCHESTRATION"
    task.status.status == "Orchestration completed."
    task.history*.status == ["Creating task 1", "Orchestration completed."]
    !task.history*.status.any { it.startsWith("Processing op: ") }
  }

  void "single op success calls complete(phase, status) and does not separately update status"() {
    setup:
    def task = Spy(DefaultTask, constructorArgs: ["1"])
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * atomicOperation.operate(_) >> "result"
    0 * task.updateStatus(_, _)
    then:
    1 * task.addResultObjects(["result"])
    then:
    1 * task.complete("ORCHESTRATION", "Orchestration completed.")
    0 * task.complete()
  }

  void "with multiple ops only the last op's completion message is merged into the completed state"() {
    setup:
    def task = Spy(DefaultTask, constructorArgs: ["1"])
    def op1 = Mock(AtomicOperation)
    def op2 = Mock(AtomicOperation)

    when:
    submitAndWait([op1, op2])

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * op1.operate(_) >> "r1"
    1 * task.updateStatus("ORCHESTRATION", "Orchestration completed.")
    then:
    1 * task.updateStatus("ORCHESTRATION", { it.startsWith("Processing op: ") })
    then:
    1 * op2.operate(_) >> "r2"
    then:
    1 * task.addResultObjects(["r1", "r2"])
    then:
    1 * task.complete("ORCHESTRATION", "Orchestration completed.")
    0 * task.updateStatus(_, _)
    0 * task.complete()
  }

  void "two successful ops write exactly one Processing op entry, for the second op"() {
    setup:
    def task = new DefaultTask("1")
    def op1 = Mock(AtomicOperation)
    def op2 = Mock(AtomicOperation)

    when:
    submitAndWait([op1, op2])

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    task.status.isCompleted()
    task.history*.status.count { it.startsWith("Processing op: ") } == 1
    task.history*.status.indexOf("Orchestration completed.") <
      task.history*.status.findIndexOf { it.startsWith("Processing op: ") }
  }

  void "a later operation does not run after an earlier one fails the task"() {
    setup:
    def task = new DefaultTask("1")
    def op1 = Mock(AtomicOperation)
    def op2 = Mock(AtomicOperation)

    when:
    submitAndWait([op1, op2])

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * op1.operate(_) >> { throw new RuntimeException("boom") }
    0 * op2.operate(_)
    task.status.isFailed()
  }

  void "an op that leaves the task failed writes completed-with-errors via updateStatus and never completes"() {
    setup:
    def failed = new DefaultTaskStatus("p", "s", TaskState.FAILED)
    def task = Mock(Task) {
      getId() >> "1"
      getStatus() >> failed
    }
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * atomicOperation.operate(_) >> null
    1 * task.updateStatus("ORCHESTRATION", "Orchestration completed with errors, see prior task logs.")
    0 * task.complete(_, _)
    0 * task.complete()
  }

  @Unroll
  void "fail the task when exception is thrown (#exception.class.simpleName, #sagaId)"() {
    setup:
    dynamicConfigService.getConfig(
      String.class,
      "clouddriver.exception-classifier.retryable-exceptions",
      'com.netflix.spinnaker.clouddriver.orchestration.DefaultOrchestrationProcessorSpec$RetryableException'
    ) >> { 'com.netflix.spinnaker.clouddriver.orchestration.DefaultOrchestrationProcessorSpec$SomeDynamicException,com.netflix.spinnaker.clouddriver.orchestration.DefaultOrchestrationProcessorSpec$AnotherDynamicException' }
    def task = new DefaultTask("1")
    if (sagaId) {
      task.sagaIdentifiers.add(sagaId)
    }
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * atomicOperation.operate(_) >> { throw exception }
    task.status.isFailed()
    task.status.isRetryable() == retryable

    //Tasks without SagaIds (i.e., not a saga) are not retryable
    where:
    exception                     | sagaId               || retryable
    new RuntimeException()        | null                 || false
    new RetryableException()      | null                 || false
    new RuntimeException()        | new SagaId("a", "a") || false
    new NonRetryableException()   | new SagaId("a", "a") || false
    new RetryableException()      | new SagaId("a", "a") || true
    new SomeDynamicException()    | new SagaId("a", "a") || true
    new AnotherDynamicException() | new SagaId("a", "a") || true
  }

  void "failure should be logged in the result objects"() {
    setup:
    def task = new DefaultTask("1")
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * atomicOperation.operate(_) >> { throw new RuntimeException(message) }
    task.resultObjects.find { it.type == "EXCEPTION" }
    task.resultObjects.find { it.type == "EXCEPTION" }.message == message

    where:
    message = "foo"
  }

  void "does not re-run existing task based on clientRequestId"() {
    def task = new DefaultTask("1")
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    taskRepository.getByClientRequestId(taskKey) >>> [null, task]
    1 * taskRepository.create(_, _, taskKey) >> task
    task.status.isCompleted()
    !task.status.isFailed()
  }

  void "should clear MDC thread local"() {
    given:
    def context = contextProvider.get()
    MDC.put("myKey", "myValue")
    context.setAccounts("myAccounts")
    context.setUser( "myUser")

    when:
    processor.clearRequestContext()

    then:
    MDC.get("myKey") == "myValue"
    !context.getAccounts().isPresent()
    !context.getUser().isPresent()
  }

  void "does not run the remaining operations after one fails"() {
    setup:
    def task = new DefaultTask("1")
    def first = Mock(AtomicOperation)
    def second = Mock(AtomicOperation)
    def third = Mock(AtomicOperation)

    when:
    submitAndWait([first, second, third])

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    1 * first.operate(_) >> [result: "first"]
    1 * second.operate(_) >> { throw new RuntimeException("boom") }
    0 * third.operate(_)
    task.status.isFailed()
    task.resultObjects.find { it.type == "EXCEPTION" }
    // Results of the operations that succeeded before the failure are kept.
    task.resultObjects.contains([result: "first"])
  }

  void "re-runs a task whose failure is retryable"() {
    setup:
    def task = new DefaultTask("1")
    task.fail(true)
    def atomicOperation = Mock(AtomicOperation)

    when:
    submitAndWait atomicOperation

    then:
    taskRepository.getByClientRequestId(taskKey) >> task
    0 * taskRepository.create(_, _, _)
    1 * atomicOperation.operate(_)
    task.status.isCompleted()
    !task.status.isFailed()
  }

  void "runs the operation with the submitting thread's security context"() {
    given:
    def task = new DefaultTask("1")
    def authentication = new TestingAuthenticationToken("alice", "N/A")
    SecurityContextHolder.getContext().setAuthentication(authentication)
    Authentication seen = null
    def atomicOperation = Mock(AtomicOperation) {
      operate(_) >> {
        seen = SecurityContextHolder.getContext().getAuthentication()
        null
      }
    }

    when:
    submitAndWait atomicOperation

    then:
    1 * taskRepository.create(_, _, taskKey) >> task
    seen == authentication

    cleanup:
    SecurityContextHolder.clearContext()
  }

  private void submitAndWait(AtomicOperation atomicOp) {
    submitAndWait([atomicOp])
  }

  private void submitAndWait(List<AtomicOperation> atomicOps) {
    processor.process("cloudProvider", atomicOps, taskKey)
    processor.executorService.shutdown()
    processor.executorService.awaitTermination(5, TimeUnit.SECONDS)
  }

  private static class NonRetryableException extends RuntimeException {}
  private static class RetryableException extends RuntimeException {}
  private static class SomeDynamicException extends RuntimeException {}
  private static class AnotherDynamicException extends RuntimeException {}
}
