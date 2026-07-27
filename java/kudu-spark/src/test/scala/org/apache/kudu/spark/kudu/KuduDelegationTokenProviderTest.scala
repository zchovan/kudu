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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.junit.After
import org.junit.Test
import org.scalatest.matchers.should.Matchers

/**
 * Submit-side unit tests for [[KuduDelegationTokenProvider]] and
 * [[KuduSparkSecurity]] that require neither a Kudu cluster nor `KuduContext`.
 *
 * The test that actually acquires credentials from a secure cluster lives in
 * [[KuduDelegationTokenProviderSecurityTest]], which brings up a mini cluster.
 */
class KuduDelegationTokenProviderTest extends Matchers {

  // UserGroupInformation.setConfiguration mutates JVM-global state; restore a
  // default (simple-auth) configuration after every test so kerberos settings
  // do not leak into other tests sharing the JVM.
  @After
  def resetUgiConfiguration(): Unit = {
    UserGroupInformation.setConfiguration(new Configuration())
  }

  @Test
  def testProviderDiscoveredViaServiceLoader(): Unit = {
    val providers = ServiceLoader
      .load(classOf[HadoopDelegationTokenProvider])
      .iterator()
      .asScala
      .toList
    assert(providers.exists(_.isInstanceOf[KuduDelegationTokenProvider]))
    assert(providers.exists(_.serviceName == "kudu"))
  }

  // With Hadoop security disabled, no delegation tokens are needed regardless of
  // whether spark.kudu.master is set. The security-enabled cases (which require a
  // resolvable Kerberos realm) live in KuduDelegationTokenProviderSecurityTest.
  @Test
  def testDelegationTokensNotRequiredWhenSecurityDisabled(): Unit = {
    val conf = new Configuration()
    conf.set("hadoop.security.authentication", "simple")
    UserGroupInformation.setConfiguration(conf)
    assert(!UserGroupInformation.isSecurityEnabled)

    val provider = new KuduDelegationTokenProvider()
    val withMaster = new SparkConf().set(KuduSparkSecurity.KuduMasterConf, "localhost:7051")
    assert(!provider.delegationTokensRequired(withMaster, new Configuration()))

    val withoutMaster = new SparkConf()
    assert(!provider.delegationTokensRequired(withoutMaster, new Configuration()))
  }

  @Test
  def testKuduSparkSecurityRoundTrip(): Unit = {
    val bytes = Array[Byte](1, 2, 3, 4, 5)
    val creds = new Credentials()
    KuduSparkSecurity.addCredentialsToHadoop(creds, bytes)
    UserGroupInformation.getCurrentUser.addCredentials(creds)

    val recovered = KuduSparkSecurity.getCredentialsFromUGI
    assert(recovered.isDefined)
    assert(recovered.get.sameElements(bytes))
  }
}
