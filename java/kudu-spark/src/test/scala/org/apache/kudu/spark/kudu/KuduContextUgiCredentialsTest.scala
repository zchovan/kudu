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

import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.scalatest.matchers.should.Matchers

/**
 * Verifies that KuduContext prefers Kudu authentication credentials supplied
 * through UserGroupInformation (by the submit-time delegation token provider)
 * and does not attempt a driver-side export when they are present.
 *
 * This test needs no Kudu cluster: with UGI credentials present the credential
 * initialization short-circuits before any client is built, which is exactly
 * the property under test. The fallback-to-export path (which does require a
 * cluster) is covered by KuduContextTest.
 */
class KuduContextUgiCredentialsTest extends Matchers {

  private var sc: SparkContext = _

  @Before
  def setUp(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("kudu-ugi-cred-test")
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)
  }

  @After
  def tearDown(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  @Test
  def testUsesUgiCredentialsWhenPresentWithoutExport(): Unit = {
    val sentinel = Array[Byte](7, 7, 7, 7)

    // Inject the credentials into a throwaway UGI rather than the current user,
    // so they cannot leak into other tests sharing this user.
    val ugi = UserGroupInformation.createRemoteUser("kudu-deleg-token-test")
    val creds = new Credentials()
    KuduSparkSecurity.addCredentialsToHadoop(creds, sentinel)
    ugi.addCredentials(creds)

    val recovered = ugi.doAs(new PrivilegedAction[Array[Byte]] {
      override def run(): Array[Byte] = {
        // A deliberately unreachable master: if KuduContext attempted a
        // driver-side export it would try to connect here and fail, rather than
        // returning the UGI-supplied bytes.
        new KuduContext("bogus-master.invalid:7051", sc).authnCredentials
      }
    })

    assert(recovered != null)
    assert(recovered.sameElements(sentinel))
  }
}
