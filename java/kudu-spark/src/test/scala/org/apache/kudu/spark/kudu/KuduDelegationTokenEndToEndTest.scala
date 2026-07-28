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

import scala.collection.JavaConverters._
import scala.annotation.meta.getter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.scalatest.matchers.should.Matchers

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.Schema
import org.apache.kudu.Type
import org.apache.kudu.test.KuduTestHarness

/**
 * End-to-end test of the delegation-token submit flow, exercising every piece
 * added across the prior commits without requiring a real YARN cluster:
 *
 *   KuduDelegationTokenProvider  (submit side, obtains + stores credentials)
 *     -> Hadoop Credentials
 *       -> UserGroupInformation  (what YARN ships to the remote driver)
 *         -> KuduContext         (runtime, prefers UGI-supplied credentials)
 *
 * The submitter's local Kerberos credentials are destroyed before KuduContext
 * runs, so the only way the scan/write can succeed is via the imported token --
 * the exact YARN-cluster-mode-with-`--proxy-user` scenario the feature targets.
 *
 * Cluster-backed (secure ExternalMiniCluster).
 */
class KuduDelegationTokenEndToEndTest extends Matchers {

  private val tableName: String = "delegation-token-e2e"

  private val schema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("val", Type.STRING).nullable(true).build()).asJava
    new Schema(columns)
  }

  private var ss: SparkSession = _

  @(Rule @getter)
  val harness = new KuduTestHarness()

  @Before
  def setUp(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("kudu-delegation-token-e2e")
      .set("spark.ui.enabled", "false")
    ss = SparkSession.builder().config(conf).getOrCreate()
  }

  @After
  def tearDown(): Unit = {
    if (ss != null) {
      ss.stop()
    }
    KuduClientCache.clearCacheForTests()
    UserGroupInformation.setConfiguration(new Configuration())
  }

  @Test
  @KuduTestHarness.EnableKerberos
  def testEndToEndDelegationTokenFlow(): Unit = {
    val masterAddresses = harness.getMasterAddressesAsString

    // 1. As an authenticated submitter, create a table and insert a row.
    harness.kinit("test-admin")
    val client = harness.getClient
    val options = new CreateTableOptions()
      .setRangePartitionColumns(List("key").asJava)
      .setNumReplicas(1)
    val table = client.createTable(tableName, schema, options)
    val setupSession = client.newSession()
    val setupInsert = table.newInsert()
    setupInsert.getRow.addInt("key", 1)
    setupInsert.getRow.addString("val", "submitter")
    setupSession.apply(setupInsert)
    setupSession.flush()

    // 2. Submit-time provider fetches Kudu credentials into a Hadoop Credentials
    //    bundle (what Spark/YARN would ship to the driver).
    val sparkConf = new SparkConf().set(KuduSparkSecurity.KuduMasterConf, masterAddresses)
    val creds = new Credentials()
    val renewal =
      new KuduDelegationTokenProvider()
        .obtainDelegationTokens(new Configuration(), sparkConf, creds)
    assert(renewal.isEmpty)
    assert(creds.getSecretKey(KuduSparkSecurity.KuduCredentialsAlias) != null)

    // 3. Simulate the remote driver: destroy the submitter's local Kerberos
    //    credentials so the imported token is the only way to authenticate, and
    //    drop any cached client so KuduContext must build a fresh one.
    harness.kdestroy()
    KuduClientCache.clearCacheForTests()

    // 4. Load the shipped credentials into a UGI (as YARN would) and, running as
    //    that user with no TGT, build a KuduContext and perform a write and a scan.
    val ugi = UserGroupInformation.createRemoteUser("alice")
    ugi.addCredentials(creds)
    val rowCount: java.lang.Long = ugi.doAs(new PrivilegedAction[java.lang.Long] {
      override def run(): java.lang.Long = {
        val kuduContext = new KuduContext(masterAddresses, ss.sparkContext)

        // Write path: a master/tserver RPC authenticated purely by the token.
        assert(kuduContext.tableExists(tableName))
        val writeTable = kuduContext.syncClient.openTable(tableName)
        val session = kuduContext.syncClient.newSession()
        val insert = writeTable.newInsert()
        insert.getRow.addInt("key", 2)
        insert.getRow.addString("val", "imported")
        session.apply(insert)
        session.flush()

        // Scan path: read the table back through Spark (the KuduContext is
        // serialized to executors, which import the same credentials).
        ss.read
          .options(Map("kudu.master" -> masterAddresses, "kudu.table" -> tableName))
          .format("kudu")
          .load
          .count()
      }
    })

    assert(rowCount == 2L)
  }
}
