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

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.yetus.audience.InterfaceAudience
import org.apache.yetus.audience.InterfaceStability

/**
 * Shared constants and helpers for shipping Kudu authentication credentials
 * through Hadoop [[Credentials]] / [[UserGroupInformation]].
 *
 * Kudu's exported authentication credentials are an opaque, already-serialized
 * byte array (authn token plus trusted CA material), not a renewable Hadoop
 * delegation token. They are therefore stored as a Hadoop secret key under a
 * stable alias rather than as a Hadoop token with a kind/service contract.
 *
 * This is used by both [[KuduDelegationTokenProvider]] (submit side, writes the
 * bytes) and `KuduContext` (runtime, reads the bytes) so the alias and config
 * key are defined in exactly one place.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
object KuduSparkSecurity {

  /**
   * Spark configuration key holding the comma-separated Kudu master addresses.
   *
   * Required at submit time: the delegation-token provider runs before user
   * code, so it cannot observe a DataFrame `.option("kudu.master", ...)`.
   */
  val KuduMasterConf: String = "spark.kudu.master"

  /**
   * Stable alias under which Kudu credential bytes are stored in Hadoop [[Credentials]].
   */
  val KuduCredentialsAlias: Text = new Text("kudu.authn.credentials")

  /**
   * Store the exported Kudu authentication credential bytes into the given
   * Hadoop [[Credentials]] under [[KuduCredentialsAlias]].
   */
  def addCredentialsToHadoop(creds: Credentials, authnData: Array[Byte]): Unit = {
    creds.addSecretKey(KuduCredentialsAlias, authnData)
  }

  /**
   * Return the Kudu authentication credential bytes stored in the current
   * user's [[UserGroupInformation]], if present and non-empty.
   */
  def getCredentialsFromUGI: Option[Array[Byte]] = {
    Option(UserGroupInformation.getCurrentUser.getCredentials.getSecretKey(KuduCredentialsAlias))
      .filter(_.nonEmpty)
  }
}
