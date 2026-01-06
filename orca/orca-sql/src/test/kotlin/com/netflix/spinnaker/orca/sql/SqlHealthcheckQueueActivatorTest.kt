/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.spinnaker.orca.sql

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.SqlHealthcheckActivatorProperties
import com.netflix.spinnaker.kork.sql.config.ConnectionPoolProperties
import com.netflix.spinnaker.kork.sql.config.SqlProperties
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.HikariPoolMXBean
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.isA
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import org.mockito.kotlin.whenever
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import org.jooq.DSLContext
import org.jooq.DeleteUsingStep
import org.jooq.Table
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue

class SqlHealthcheckQueueActivatorTest : JUnit5Minutests {

  fun tests() = rootContext<Unit> {

    val dslContext = mock<DSLContext>()
    val query = mock<DeleteUsingStep<*>>()
    val dataSource = mock<HikariDataSource>()
    val hikariPool = mock<HikariPoolMXBean>()

    // Create a mock SqlProperties with default connection pool
    val sqlProperties = SqlProperties().apply {
      connectionPools = mutableMapOf(
        "default" to ConnectionPoolProperties().apply {
          minIdle = 10
          maxPoolSize = 100
          default = true
        }
      )
    }

    after {
      reset(dslContext, query, dataSource, hikariPool)
    }

    context("basic health check functionality") {
      val properties = SqlHealthcheckActivatorProperties().apply {
        unhealthyThreshold = 1
        healthyThreshold = 1
      }

      test("successive write failures disable activator") {
        whenever(dataSource.hikariPoolMXBean) doReturn hikariPool
        whenever(hikariPool.totalConnections) doReturn 10
        whenever(hikariPool.activeConnections) doReturn 5
        whenever(hikariPool.idleConnections) doReturn 5
        whenever(hikariPool.threadsAwaitingConnection) doReturn 0
        whenever(dslContext.delete(isA<Table<*>>())) doThrow RuntimeException("oh no")

        val subject = SqlHealthcheckActivator(dslContext, NoopRegistry(), dataSource, properties, sqlProperties).apply {
          enabledAtomic.set(true)
          initialWarmupCompleteAtomic.set(true)
        }

        subject.performWrite()

        expectThat(subject.enabled).isFalse()
      }

      test("successive write successes enable activator") {
        whenever(dataSource.hikariPoolMXBean) doReturn hikariPool
        whenever(hikariPool.totalConnections) doReturn 10
        whenever(hikariPool.activeConnections) doReturn 5
        whenever(hikariPool.idleConnections) doReturn 5
        whenever(hikariPool.threadsAwaitingConnection) doReturn 0
        whenever(dslContext.delete(isA<Table<*>>())) doReturn query

        val subject = SqlHealthcheckActivator(dslContext, NoopRegistry(), dataSource, properties, sqlProperties).apply {
          enabledAtomic.set(false)
        }

        subject.performWrite()

        expectThat(subject.enabled).isTrue()
      }
    }

    context("initial warmup - minIdle check") {
      val properties = SqlHealthcheckActivatorProperties().apply {
        unhealthyThreshold = 2
        healthyThreshold = 2
      }

      test("blocks enable until minIdle connections are established") {
        // Update sqlProperties for this test
        val testSqlProperties = SqlProperties().apply {
          connectionPools = mutableMapOf(
            "default" to ConnectionPoolProperties().apply {
              minIdle = 300
              maxPoolSize = 600
              default = true
            }
          )
        }

        whenever(dataSource.hikariPoolMXBean) doReturn hikariPool
        whenever(hikariPool.totalConnections) doReturn 150  // Only 150 out of 300
        whenever(hikariPool.activeConnections) doReturn 50
        whenever(hikariPool.idleConnections) doReturn 100
        whenever(hikariPool.threadsAwaitingConnection) doReturn 0
        whenever(dslContext.delete(isA<Table<*>>())) doReturn query

        val subject = SqlHealthcheckActivator(dslContext, NoopRegistry(), dataSource, properties, testSqlProperties).apply {
          enabledAtomic.set(false)
        }

        subject.performWrite()
        subject.performWrite()

        expectThat(subject.enabled).isFalse()
      }
    }

    context("threshold configuration") {
      test("uses configured unhealthy threshold") {
        val properties = SqlHealthcheckActivatorProperties().apply {
          unhealthyThreshold = 3
          healthyThreshold = 1
        }

        whenever(dataSource.hikariPoolMXBean) doReturn hikariPool
        whenever(hikariPool.totalConnections) doReturn 10
        whenever(hikariPool.activeConnections) doReturn 5
        whenever(hikariPool.idleConnections) doReturn 5
        whenever(hikariPool.threadsAwaitingConnection) doReturn 0
        whenever(dslContext.delete(isA<Table<*>>())) doThrow RuntimeException("failure")

        val subject = SqlHealthcheckActivator(dslContext, NoopRegistry(), dataSource, properties, sqlProperties).apply {
          enabledAtomic.set(true)
          initialWarmupCompleteAtomic.set(true)
        }

        // First two failures shouldn't disable (threshold is 3)
        subject.performWrite()
        expectThat(subject.enabled).isTrue()
        subject.performWrite()
        expectThat(subject.enabled).isTrue()

        // Third failure should disable
        subject.performWrite()
        expectThat(subject.enabled).isFalse()
      }

      test("uses configured healthy threshold") {
        val properties = SqlHealthcheckActivatorProperties().apply {
          unhealthyThreshold = 1
          healthyThreshold = 3
        }

        whenever(dataSource.hikariPoolMXBean) doReturn hikariPool
        whenever(hikariPool.totalConnections) doReturn 10
        whenever(hikariPool.activeConnections) doReturn 5
        whenever(hikariPool.idleConnections) doReturn 5
        whenever(hikariPool.threadsAwaitingConnection) doReturn 0
        whenever(dslContext.delete(isA<Table<*>>())) doReturn query

        val subject = SqlHealthcheckActivator(dslContext, NoopRegistry(), dataSource, properties, sqlProperties).apply {
          enabledAtomic.set(false)
        }

        // First two successes shouldn't enable (threshold is 3)
        subject.performWrite()
        expectThat(subject.enabled).isFalse()
        subject.performWrite()
        expectThat(subject.enabled).isFalse()

        // Third success should enable
        subject.performWrite()
        expectThat(subject.enabled).isTrue()
      }
    }
  }
}
