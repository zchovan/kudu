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

package org.apache.kudu.replication;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.shaded.guava31.com.google.common.base.Preconditions.checkNotNull;

/**
 * A configuration object for ReplicationJobs used for the Kudu Flink based replication.
 */
public class ReplicationJobConfig implements Serializable {
  private final String sourceMasterAddresses;
  private final String sinkMasterAddresses;
  private final String tableName;
  private final boolean restoreOwner;
  private final String tableSuffix;
  private final long disoveryIntervalSeconds;

  private ReplicationJobConfig(
          String sourceMasterAddresses,
          String sinkMasterAddresses,
          String tableName,
          boolean restoreOwner,
          String tableSuffix,
          long disoveryIntervalSeconds) {
    this.sourceMasterAddresses = checkNotNull(sourceMasterAddresses, "sourceMasterAddresses cannot be null");
    this.sinkMasterAddresses = checkNotNull(sinkMasterAddresses, "sinkMasterAddresses cannot be null");
    this.tableName = checkNotNull(tableName, "tableName cannot be null");
    this.restoreOwner = restoreOwner;
    this.tableSuffix = tableSuffix != null ? tableSuffix : "";
    this.disoveryIntervalSeconds = disoveryIntervalSeconds;
  }

  public String getSourceMasterAddresses() {
    return sourceMasterAddresses;
  }

  public String getSinkMasterAddresses() {
    return sinkMasterAddresses;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean getRestoreOwner() {
    return restoreOwner;
  }

  public String getTableSuffix() {
    return tableSuffix;
  }

  public long getDisoveryIntervalSeconds() {
    return disoveryIntervalSeconds;
  }

  public String getSinkTableName() {
    return tableName + tableSuffix;
  }

  /** Builder for {@link ReplicationJobConfig}. */
  public static class Builder {
    private String sourceMasterAddresses;
    private String sinkMasterAddresses;
    private String tableName;
    private boolean restoreOwner = false;
    private String tableSuffix = "";
    private long disoveryIntervalSeconds = 15 * 60;

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder setSourceMasterAddresses(String sourceMasterAddresses) {
      this.sourceMasterAddresses = sourceMasterAddresses;
      return this;
    }

    public Builder setSinkMasterAddresses(String sinkMasterAddresses) {
      this.sinkMasterAddresses = sinkMasterAddresses;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setRestoreOwner(boolean restoreOwner) {
      this.restoreOwner = restoreOwner;
      return this;
    }

    public Builder setTableSuffix(String tableSuffix) {
      this.tableSuffix = tableSuffix;
      return this;
    }

    public Builder setDisoveryIntervalSeconds(long disoveryIntervalSeconds) {
      this.disoveryIntervalSeconds = disoveryIntervalSeconds;
      return this;
    }

    public ReplicationJobConfig build() {
      return new ReplicationJobConfig(
              sourceMasterAddresses, sinkMasterAddresses, tableName, restoreOwner, tableSuffix, disoveryIntervalSeconds);
    }
  }
}
