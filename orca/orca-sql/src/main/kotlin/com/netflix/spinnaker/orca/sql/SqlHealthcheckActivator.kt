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

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.SqlHealthcheckActivatorProperties
import com.netflix.spinnaker.kork.sql.config.SqlProperties
import com.netflix.spinnaker.q.Activator
import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource
import org.jooq.DSLContext
import org.jooq.impl.DSL.table
import org.slf4j.LoggerFactory
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource
import org.springframework.scheduling.annotation.Scheduled

/**
 * Continually performs writes to the SQL backend in order to detect master
 * failures and to deactivate queue processing as a result.
 *
 * Behaves quite like load balancer healthchecks. By default, this activator
 * will fail fast and be slow to come healthy again.
 *
 * Enhanced to check connection pool health:
 * - During startup: Waits for minIdle connections to be established
 */
class SqlHealthcheckActivator(
  private val jooq: DSLContext,
  private val registry: Registry,
  private val dataSource: DataSource,
  private val properties: SqlHealthcheckActivatorProperties,
  private val sqlProperties: SqlProperties
) : Activator {

  private val log = LoggerFactory.getLogger(javaClass)

  internal val enabledAtomic = AtomicBoolean(false)
  private val healthExceptionAtomic: AtomicReference<Exception> = AtomicReference()
  internal val initialWarmupCompleteAtomic = AtomicBoolean(false)

  private val healthyCounter = AtomicInteger(0)
  private val unhealthyCounter = AtomicInteger(0)

  private val warmupStartTimeMs = System.currentTimeMillis()
  private val warmupCompleteTimeMs = AtomicReference<Long?>(null)

  private val invocationId = registry.createId("sql.queueActivator.invocations")
  private val warmupDurationId = registry.createId("sql.queueActivator.warmupDuration")

  override val enabled: Boolean
    get() = enabledAtomic.get()

  val healthException: Exception?
    get() = healthExceptionAtomic.get()

  @Scheduled(fixedDelayString = "\${sql.healthcheck.interval-ms:1000}")
  fun performWrite() {
    try {
      verifyMinIdleConnectionsHaveBeenEstablished()

      // Check database responsiveness
      jooq.delete(table("healthcheck")).execute()

      // Success path
      if (!enabledAtomic.get()) {
        if (healthyCounter.incrementAndGet() >= properties.healthyThreshold) {
          enabledAtomic.set(true)
          healthExceptionAtomic.set(null)

          // Track warmup completion on first enable
          if (!initialWarmupCompleteAtomic.get()) {
            initialWarmupCompleteAtomic.set(true)
            val warmupDurationMs = System.currentTimeMillis() - warmupStartTimeMs
            warmupCompleteTimeMs.set(warmupDurationMs)

            // Record warmup duration as a gauge (in seconds)
            registry.gauge(warmupDurationId, warmupDurationMs / 1000.0)

            log.info("Enabling queue processing after ${properties.healthyThreshold} healthy cycles: connection pool ready, database responsive (warmup took ${warmupDurationMs}ms)")
          } else {
            log.info("Re-enabling queue processing after ${properties.healthyThreshold} healthy cycles")
          }
        }
      }
      unhealthyCounter.set(0)

    } catch (e: Exception) {
      healthExceptionAtomic.set(e)
      healthyCounter.set(0)
      unhealthyCounter.incrementAndGet().also { unhealthyCount ->
        log.error("Health check failed, $unhealthyCount/${properties.unhealthyThreshold} failures", e)
        if (unhealthyCount >= properties.unhealthyThreshold && enabledAtomic.get()) {
          log.warn("Disabling queue processing after $unhealthyCount consecutive failures")
          enabledAtomic.set(false)
        }
      }
    } finally {
      registry.counter(invocationId.withTag("status", if (enabled) "enabled" else "disabled")).increment()
    }
  }

  /**
   * Verify that the minimum number of idle connections have been established. This
   * prevents issues where queue processing starts before the connection pool is fully
   * initialized.
   *
   * Unwraps the DataSource to get real-time metrics from HikariCP, which works with
   * both single and multiple connection pool configurations.
   */
  fun verifyMinIdleConnectionsHaveBeenEstablished() {
    // Get configured pool properties
    val poolConfig = sqlProperties.getDefaultConnectionPoolProperties()
    val configuredMinIdle = poolConfig.minIdle
    val configuredMaxPoolSize = poolConfig.maxPoolSize

    // Unwrap the DataSource to get the actual HikariDataSource
    val hikariDataSource = unwrapDataSource(dataSource)
    if (hikariDataSource == null) {
      log.warn("Unable to unwrap DataSource to HikariDataSource - skipping pool health checks")
      return
    }

    val hikariPool = hikariDataSource.hikariPoolMXBean
    if (hikariPool == null) {
      log.warn("HikariPoolMXBean not available - skipping pool health checks")
      return
    }

    // Get real-time metrics from HikariCP
    val totalConnections = hikariPool.totalConnections
    val activeConnections = hikariPool.activeConnections
    val idleConnections = hikariPool.idleConnections
    val threadsWaiting = hikariPool.threadsAwaitingConnection

    // RUNTIME CHECK: Monitor utilization
    val utilization = if (totalConnections > 0) {
      (activeConnections.toDouble() / totalConnections) * 100
    } else {
      0.0
    }

    log.debug("Connection pool status: total=$totalConnections, active=$activeConnections, idle=$idleConnections, utilization=$utilization%, configured minIdle=$configuredMinIdle, maxPoolSize=$configuredMaxPoolSize, threadsWaiting=$threadsWaiting")

    // STARTUP CHECK: Wait for minIdle connections before first enable
    if (!initialWarmupCompleteAtomic.get() && totalConnections < configuredMinIdle) {
      log.info("Connection pool warm up in progress: $totalConnections/$configuredMinIdle connections established")
      throw Exception("Connection pool initializing: $totalConnections/$configuredMinIdle connections established")
    }

    // RUNTIME CHECK: Disable if threads are waiting for connections
    if (threadsWaiting > 0) {
      log.warn("Connection pool exhausted: $threadsWaiting threads waiting for connections")
      throw Exception("Connection pool exhausted: $threadsWaiting threads waiting for connections")
    }
  }

  /**
   * Unwrap the DataSource to get the underlying HikariDataSource.
   * Handles both direct HikariDataSource and AbstractRoutingDataSource (NamedDataSourceRouter).
   */
  private fun unwrapDataSource(ds: DataSource): HikariDataSource? {
    return when (ds) {
      is HikariDataSource -> ds
      is AbstractRoutingDataSource -> {
        // Get the resolved default DataSource from the router
        val defaultDataSource = ds.resolvedDefaultDataSource
        if (defaultDataSource is HikariDataSource) {
          defaultDataSource
        } else {
          log.warn("Resolved default DataSource is not a HikariDataSource: ${defaultDataSource?.javaClass?.name}")
          null
        }
      }
      else -> {
        log.warn("DataSource is not HikariDataSource or AbstractRoutingDataSource: ${ds.javaClass.name}")
        null
      }
    }
  }
}
