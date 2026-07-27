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

import scala.annotation.meta.getter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.junit.After
import org.junit.Rule
import org.junit.Test
import org.scalatest.matchers.should.Matchers

import org.apache.kudu.test.KuduTestHarness

/**
 * Secure, cluster-backed test for [[KuduDelegationTokenProvider]]: verifies that
 * the provider exports Kudu authentication credentials against a Kerberized mini
 * cluster and stores them in Hadoop [[Credentials]].
 */
class KuduDelegationTokenProviderSecurityTest extends Matchers {

  // Ensure the annotation is applied to the getter and not the field
  // or else JUnit will complain that the Rule must be public.
  @(Rule @getter)
  val harness = new KuduTestHarness()

  // UserGroupInformation.setConfiguration mutates JVM-global state; restore a
  // default (simple-auth) configuration after every test so kerberos settings do
  // not leak into other tests sharing the JVM.
  @After
  def resetUgiConfiguration(): Unit = {
    UserGroupInformation.setConfiguration(new Configuration())
  }

  // With Hadoop security enabled, delegation tokens are required exactly when
  // spark.kudu.master is set. This runs under @EnableKerberos so the mini KDC's
  // krb5.conf makes the Kerberos realm resolvable (UserGroupInformation.setConfiguration
  // fails without a resolvable realm), which is why it is not a plain unit test.
  @Test
  @KuduTestHarness.EnableKerberos
  def testDelegationTokensRequiredReflectsMasterConf(): Unit = {
    val krbConf = new Configuration()
    krbConf.set("hadoop.security.authentication", "kerberos")
    UserGroupInformation.setConfiguration(krbConf)
    assert(UserGroupInformation.isSecurityEnabled)

    val provider = new KuduDelegationTokenProvider()
    val withMaster = new SparkConf()
      .set(KuduSparkSecurity.KuduMasterConf, harness.getMasterAddressesAsString)
    assert(provider.delegationTokensRequired(withMaster, new Configuration()))

    val withoutMaster = new SparkConf()
    assert(!provider.delegationTokensRequired(withoutMaster, new Configuration()))
  }

  @Test
  @KuduTestHarness.EnableKerberos
  def testObtainDelegationTokensWritesCredentials(): Unit = {
    harness.kinit("test-admin")

    val sparkConf = new SparkConf()
      .set(KuduSparkSecurity.KuduMasterConf, harness.getMasterAddressesAsString)
    val creds = new Credentials()
    val provider = new KuduDelegationTokenProvider()

    val renewal = provider.obtainDelegationTokens(new Configuration(), sparkConf, creds)

    // No renewal in the first pass.
    assert(renewal.isEmpty)

    // The exported Kudu credentials were stored under the shared alias.
    val stored = creds.getSecretKey(KuduSparkSecurity.KuduCredentialsAlias)
    assert(stored != null)
    assert(stored.nonEmpty)
  }
}
