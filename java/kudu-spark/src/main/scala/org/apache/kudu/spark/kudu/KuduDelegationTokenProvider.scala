// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.spark.kudu

import java.security.PrivilegedAction

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.kudu.client.KuduClient

/**
 * A Spark [[HadoopDelegationTokenProvider]] that fetches Kudu authentication
 * credentials at application submission time.
 *
 * Spark discovers this provider through the Java `ServiceLoader` (see
 * `META-INF/services/org.apache.spark.security.HadoopDelegationTokenProvider`),
 * so no patch to Spark is required -- only the Kudu Spark connector jar on the
 * submit classpath.
 *
 * The provider runs while the submitter still holds Kerberos credentials. It
 * builds a short-lived Kudu client, exports the Kudu authentication credentials,
 * and stores them in the Hadoop [[Credentials]] bundle that Spark/YARN ships to
 * the remote driver and executors. This enables YARN cluster mode with
 * `--proxy-user` and no keytab, where the remote driver has no TGT of its own.
 *
 * Disable with the standard Spark switch:
 * {{{ spark.security.credentials.kudu.enabled=false }}}
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class KuduDelegationTokenProvider extends HadoopDelegationTokenProvider {

  private val log: Logger = LoggerFactory.getLogger(classOf[KuduDelegationTokenProvider])

  // Timeouts for the short-lived submit-time client. Kept explicit and small so
  // a slow or unreachable master fails fast rather than stalling submission.
  private val AdminOperationTimeoutMs: Long = 30000
  private val ConnectionNegotiationTimeoutMs: Long = 10000

  override def serviceName: String = "kudu"

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled &&
    sparkConf.getOption(KuduSparkSecurity.KuduMasterConf).exists(_.nonEmpty)
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    val masterAddresses = sparkConf.get(KuduSparkSecurity.KuduMasterConf)
    log.info(s"Obtaining Kudu authentication credentials from masters: $masterAddresses")

    // Run under the submitter's UGI so the short-lived client authenticates with
    // the submitter's Kerberos credentials.
    val authnCredentials =
      UserGroupInformation.getCurrentUser.doAs(new PrivilegedAction[Array[Byte]] {
        override def run(): Array[Byte] = {
          val client = new KuduClient.KuduClientBuilder(masterAddresses)
            .defaultAdminOperationTimeoutMs(AdminOperationTimeoutMs)
            .connectionNegotiationTimeoutMs(ConnectionNegotiationTimeoutMs)
            .workerCount(1)
            .build()
          try {
            // No catch: if credentials cannot be obtained (e.g. masters
            // unreachable, or the submitter has no valid Kerberos ticket), let
            // the exception propagate so submission fails fast rather than
            // shipping no credentials and failing later on the remote driver.
            client.exportAuthenticationCredentials()
          } finally {
            // Close in a nested try so a failure here cannot mask (replace) a
            // KuduException already thrown by exportAuthenticationCredentials --
            // plain try/finally does not record it as suppressed. close() also
            // throws KuduException, so guard it and only log.
            try {
              client.close()
            } catch {
              case NonFatal(e) =>
                log.warn("Failed to close the temporary Kudu client", e)
            }
          }
        }
      })

    KuduSparkSecurity.addCredentialsToHadoop(creds, authnCredentials)
    log.info("Stored Kudu authentication credentials in Hadoop credentials")

    // Kudu authn credentials are not modeled as renewable Hadoop delegation
    // tokens, so there is no renewal time to return.
    None
  }
}
