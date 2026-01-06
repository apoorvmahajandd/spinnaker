/*
 * Copyright 2025 DoorDash, Inc.
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
package com.netflix.spinnaker.config

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Configuration properties for SQL healthcheck activator behavior
 */
@ConfigurationProperties("sql.healthcheck")
class SqlHealthcheckActivatorProperties {
  /**
   * Interval in milliseconds between health checks
   */
  var intervalMs: Long = 1000

  /**
   * Number of consecutive failures before disabling queue processing
   */
  var unhealthyThreshold: Int = 2

  /**
   * Number of consecutive successes required before enabling queue processing
   */
  var healthyThreshold: Int = 10
}

