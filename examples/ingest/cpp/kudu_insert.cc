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

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <ctime>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <thread>

#include <kudu/client/client.h>
#include <kudu/client/write_op.h>
#include <kudu/common/partial_row.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::Status;

static const char* const kTableName = "default.hits";
static const int kNumReplicas = 1;
static const int kDefaultMetricsPort = 8080;
static const char* const kMasterEnvVar = "KUDU_MASTER";
static const char* const kMetricsEnvVar = "KUDU_INGEST_METRICS_PORT";
static const char* const kMetricsOutputEnvVar = "KUDU_INGEST_METRICS_OUTPUT";
static const char* const kMetricsKeepaliveEnvVar = "KUDU_INGEST_METRICS_KEEPALIVE_SECONDS";

struct AppConfig {
  std::string file_path;
  std::string master_address;
  int metrics_port;
  std::string metrics_output_path;
  int metrics_keepalive_seconds;
  bool metrics_keepalive_forever;
};

struct IngestResult {
  int64_t rows_read = 0;
  int64_t rows_inserted = 0;
  int64_t error_count = 0;
};

struct IngestMetrics {
  std::shared_ptr<prometheus::Registry> registry;
  std::unique_ptr<prometheus::Exposer> exposer;
  prometheus::Counter* rows_read = nullptr;
  prometheus::Counter* rows_inserted = nullptr;
  prometheus::Counter* error_count = nullptr;
  prometheus::Gauge* elapsed_seconds = nullptr;
  prometheus::Gauge* rows_per_second = nullptr;
};

static std::string GetEnvOrDefault(const char* name,
                                   const std::string& default_value) {
  const char* raw_value = std::getenv(name);
  if (raw_value == nullptr || std::string(raw_value).empty()) {
    return default_value;
  }
  return std::string(raw_value);
}

static bool ParseIntValue(const std::string& value, int* out) {
  try {
    size_t idx = 0;
    int parsed = std::stoi(value, &idx);
    if (idx != value.size()) {
      return false;
    }
    *out = parsed;
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

static int GetEnvOrDefaultInt(const char* name, int default_value) {
  const char* raw_value = std::getenv(name);
  if (raw_value == nullptr || std::string(raw_value).empty()) {
    return default_value;
  }
  int parsed = default_value;
  if (ParseIntValue(raw_value, &parsed)) {
    return parsed;
  }
  return default_value;
}

static void PrintUsage(const char* binary) {
  std::cerr << "Usage: " << binary << " <tsv_file_path> [master_address]"
            << " [--master <host:port>] [--metrics-port <port>]" << std::endl;
  std::cerr << "  tsv_file_path   - Path to the TSV file to ingest" << std::endl;
  std::cerr << "  master_address  - Kudu master address (default: localhost:7051"
            << " or $KUDU_MASTER)" << std::endl;
  std::cerr << "  --metrics-port  - Port for /metrics endpoint (default: "
            << kDefaultMetricsPort << " or $KUDU_INGEST_METRICS_PORT)"
            << std::endl;
  std::cerr << "  --metrics-output - Write JSON metrics summary to a file"
            << " (default: $KUDU_INGEST_METRICS_OUTPUT)" << std::endl;
  std::cerr << "  --metrics-keepalive-seconds - Keep metrics endpoint alive"
            << " after ingest (default: $KUDU_INGEST_METRICS_KEEPALIVE_SECONDS)"
            << std::endl;
  std::cerr << "  --metrics-keepalive - Keep metrics endpoint alive indefinitely"
            << std::endl;
}

static bool ParseArgs(int argc, char* argv[], AppConfig* config) {
  if (argc < 2) {
    return false;
  }

  config->file_path = argv[1];
  config->master_address = GetEnvOrDefault(kMasterEnvVar, "localhost:7051");
  config->metrics_port = GetEnvOrDefaultInt(kMetricsEnvVar, kDefaultMetricsPort);
  config->metrics_output_path = GetEnvOrDefault(kMetricsOutputEnvVar, "");
  config->metrics_keepalive_seconds = GetEnvOrDefaultInt(kMetricsKeepaliveEnvVar, 0);
  config->metrics_keepalive_forever = false;

  bool master_set = false;
  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      return false;
    }
    if (arg == "--master" && i + 1 < argc) {
      config->master_address = argv[++i];
      master_set = true;
      continue;
    }
    if (arg.rfind("--master=", 0) == 0) {
      config->master_address = arg.substr(std::strlen("--master="));
      master_set = true;
      continue;
    }
    if (arg == "--metrics-port" && i + 1 < argc) {
      int parsed = 0;
      if (!ParseIntValue(argv[++i], &parsed)) {
        return false;
      }
      config->metrics_port = parsed;
      continue;
    }
    if (arg.rfind("--metrics-port=", 0) == 0) {
      int parsed = 0;
      if (!ParseIntValue(arg.substr(std::strlen("--metrics-port=")), &parsed)) {
        return false;
      }
      config->metrics_port = parsed;
      continue;
    }
    if (arg == "--metrics-output" && i + 1 < argc) {
      config->metrics_output_path = argv[++i];
      continue;
    }
    if (arg.rfind("--metrics-output=", 0) == 0) {
      config->metrics_output_path = arg.substr(std::strlen("--metrics-output="));
      continue;
    }
    if (arg == "--metrics-keepalive" || arg == "--metrics-keepalive-forever") {
      config->metrics_keepalive_forever = true;
      continue;
    }
    if (arg == "--metrics-keepalive-seconds" && i + 1 < argc) {
      int parsed = 0;
      if (!ParseIntValue(argv[++i], &parsed)) {
        return false;
      }
      config->metrics_keepalive_seconds = parsed;
      continue;
    }
    if (arg.rfind("--metrics-keepalive-seconds=", 0) == 0) {
      int parsed = 0;
      if (!ParseIntValue(arg.substr(std::strlen("--metrics-keepalive-seconds=")), &parsed)) {
        return false;
      }
      config->metrics_keepalive_seconds = parsed;
      continue;
    }
    if (arg.rfind("--", 0) != 0 && !master_set) {
      config->master_address = arg;
      master_set = true;
      continue;
    }
    return false;
  }

  return true;
}

static std::unique_ptr<IngestMetrics> StartMetrics(int metrics_port) {
  if (metrics_port <= 0) {
    return nullptr;
  }

  auto metrics = std::make_unique<IngestMetrics>();
  metrics->registry = std::make_shared<prometheus::Registry>();
  metrics->exposer = std::make_unique<prometheus::Exposer>(
      "0.0.0.0:" + std::to_string(metrics_port));
  metrics->exposer->RegisterCollectable(metrics->registry);

  auto& rows_read_family = prometheus::BuildCounter()
                               .Name("kudu_ingest_rows_read")
                               .Help("Rows read from TSV")
                               .Register(*metrics->registry);
  metrics->rows_read = &rows_read_family.Add({});

  auto& rows_inserted_family = prometheus::BuildCounter()
                                   .Name("kudu_ingest_rows_inserted")
                                   .Help("Rows successfully inserted")
                                   .Register(*metrics->registry);
  metrics->rows_inserted = &rows_inserted_family.Add({});

  auto& error_family = prometheus::BuildCounter()
                           .Name("kudu_ingest_error_count")
                           .Help("Total error count")
                           .Register(*metrics->registry);
  metrics->error_count = &error_family.Add({});

  auto& elapsed_family = prometheus::BuildGauge()
                             .Name("kudu_ingest_elapsed_seconds")
                             .Help("Elapsed seconds for ingest")
                             .Register(*metrics->registry);
  metrics->elapsed_seconds = &elapsed_family.Add({});

  auto& throughput_family = prometheus::BuildGauge()
                                .Name("kudu_ingest_rows_per_second")
                                .Help("Rows per second")
                                .Register(*metrics->registry);
  metrics->rows_per_second = &throughput_family.Add({});

  return metrics;
}

static void WriteMetricsSummary(const std::string& path,
                                const IngestResult& result,
                                double elapsed_seconds,
                                double rows_per_second) {
  if (path.empty()) {
    return;
  }
  std::ofstream out(path.c_str());
  if (!out.is_open()) {
    std::cerr << "Failed to write metrics summary to " << path << std::endl;
    return;
  }
  out << "{"
      << "\"rows_read\":" << result.rows_read << ","
      << "\"rows_inserted\":" << result.rows_inserted << ","
      << "\"error_count\":" << result.error_count << ","
      << "\"elapsed_seconds\":" << elapsed_seconds << ","
      << "\"rows_per_second\":" << rows_per_second
      << "}" << std::endl;
}

static void KeepMetricsAlive(const AppConfig& config) {
  if (!config.metrics_keepalive_forever && config.metrics_keepalive_seconds <= 0) {
    return;
  }
  if (config.metrics_keepalive_forever) {
    std::cout << "Keeping metrics endpoint alive indefinitely..." << std::endl;
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(60));
    }
  }
  std::cout << "Keeping metrics endpoint alive for "
            << config.metrics_keepalive_seconds << " seconds..." << std::endl;
  std::this_thread::sleep_for(
      std::chrono::seconds(config.metrics_keepalive_seconds));
}

// Helper function to parse int64 from string
static int64_t ParseInt64(const std::string& str) {
  return std::stoll(str);
}

// Helper function to parse int32 from string
static int32_t ParseInt32(const std::string& str) {
  return std::stoi(str);
}

// Helper function to parse int16 from string
static int16_t ParseInt16(const std::string& str) {
  return static_cast<int16_t>(std::stoi(str));
}

// Split a line by tab character
static std::vector<std::string> SplitByTab(const std::string& line) {
  std::vector<std::string> result;
  std::stringstream ss(line);
  std::string item; 
  
  while (std::getline(ss, item, '\t')) {
    result.push_back(item);
  }
  
  return result;
}

// Create the table schema matching FastData schema
static Status CreateSchema(KuduSchema* schema) {
  KuduSchemaBuilder schema_builder;
  
  // Primary key columns (in order)
  schema_builder.AddColumn("CounterID")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("EventDate")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("UserID")->Type(KuduColumnSchema::INT64)->NotNull();
  schema_builder.AddColumn("EventTime")->Type(KuduColumnSchema::UNIXTIME_MICROS)->NotNull();
  schema_builder.AddColumn("WatchID")->Type(KuduColumnSchema::INT64)->NotNull();
  
  schema_builder.SetPrimaryKey({"CounterID", "EventDate", "UserID", "EventTime", "WatchID"});
  
  // Non-key columns
  schema_builder.AddColumn("JavaEnable")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("Title")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("GoodEvent")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("ClientIP")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("RegionID")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("CounterClass")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("OS")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("UserAgent")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("URL")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("Referer")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("IsRefresh")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("RefererCategoryID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("RefererRegionID")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("URLCategoryID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("URLRegionID")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("ResolutionWidth")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("ResolutionHeight")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("ResolutionDepth")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("FlashMajor")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("FlashMinor")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("FlashMinor2")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("NetMajor")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("NetMinor")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("UserAgentMajor")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("UserAgentMinor")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("CookieEnable")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("JavascriptEnable")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("IsMobile")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("MobilePhone")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("MobilePhoneModel")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("Params")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("IPNetworkID")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("TraficSourceID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("SearchEngineID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("SearchPhrase")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("AdvEngineID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("IsArtifical")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("WindowClientWidth")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("WindowClientHeight")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("ClientTimeZone")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("ClientEventTime")->Type(KuduColumnSchema::UNIXTIME_MICROS)->NotNull();
  schema_builder.AddColumn("SilverlightVersion1")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("SilverlightVersion2")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("SilverlightVersion3")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("SilverlightVersion4")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("PageCharset")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("CodeVersion")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("IsLink")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("IsDownload")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("IsNotBounce")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("FUniqID")->Type(KuduColumnSchema::INT64)->NotNull();
  schema_builder.AddColumn("OriginalURL")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("HID")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("IsOldCounter")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("IsEvent")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("IsParameter")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("DontCountHits")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("WithHash")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("HitColor")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("LocalEventTime")->Type(KuduColumnSchema::UNIXTIME_MICROS)->NotNull();
  schema_builder.AddColumn("Age")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("Sex")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("Income")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("Interests")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("Robotness")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("RemoteIP")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("WindowName")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("OpenerName")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("HistoryLength")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("BrowserLanguage")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("BrowserCountry")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("SocialNetwork")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("SocialAction")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("HTTPError")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("SendTiming")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("DNSTiming")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("ConnectTiming")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("ResponseStartTiming")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("ResponseEndTiming")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("FetchTiming")->Type(KuduColumnSchema::INT32)->NotNull();
  schema_builder.AddColumn("SocialSourceNetworkID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("SocialSourcePage")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("ParamPrice")->Type(KuduColumnSchema::INT64)->NotNull();
  schema_builder.AddColumn("ParamOrderID")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("ParamCurrency")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("ParamCurrencyID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("OpenstatServiceName")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("OpenstatCampaignID")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("OpenstatAdID")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("OpenstatSourceID")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("UTMSource")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("UTMMedium")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("UTMCampaign")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("UTMContent")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("UTMTerm")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("FromTag")->Type(KuduColumnSchema::STRING)->NotNull();
  schema_builder.AddColumn("HasGCLID")->Type(KuduColumnSchema::INT16)->NotNull();
  schema_builder.AddColumn("RefererHash")->Type(KuduColumnSchema::INT64)->NotNull();
  schema_builder.AddColumn("URLHash")->Type(KuduColumnSchema::INT64)->NotNull();
  schema_builder.AddColumn("CLID")->Type(KuduColumnSchema::INT32)->NotNull();

  return schema_builder.Build(schema);
}

// Create the Kudu table
static Status CreateTable(const shared_ptr<KuduClient>& client) {
  // Check if table already exists
  bool table_exists = false;
  Status s = client->TableExists(kTableName, &table_exists);
  if (!s.ok()) {
    return s;
  }
  
  if (table_exists) {
    std::cout << "Table '" << kTableName << "' already exists" << std::endl;
    return Status::OK();
  }
  
  std::cout << "Creating table '" << kTableName << "'..." << std::endl;
  
  // Create schema
  KuduSchema schema;
  KUDU_RETURN_NOT_OK(CreateSchema(&schema));
  
  // Create table with hash partitioning
  std::unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  
  s = table_creator->table_name(kTableName)
    .schema(&schema)
    .add_hash_partitions({"CounterID", "EventDate", "UserID"}, 8)  // 8 hash buckets
    .set_range_partition_columns({"EventDate"})
    .num_replicas(kNumReplicas)
    .Create();
  
  if (!s.ok()) {
    return s;
  }
  
  std::cout << "Table '" << kTableName << "' created successfully" << std::endl;
  return Status::OK();
}

// Insert data from TSV file into the table
static Status InsertDataFromFile(const shared_ptr<KuduClient>& client,
                                   const std::string& file_path,
                                   IngestMetrics* metrics,
                                   IngestResult* result) {
  std::cout << "\nReading data from file: " << file_path << std::endl;
  
  // Open the file
  std::ifstream infile(file_path);
  if (!infile.is_open()) {
    return Status::IOError("Failed to open file: " + file_path);
  }
  
  // Open the table
  shared_ptr<KuduTable> table;
  KUDU_RETURN_NOT_OK(client->OpenTable(kTableName, &table));
  
  // Create a session
  shared_ptr<KuduSession> session = client->NewSession();
  
  // Set flush mode to MANUAL_FLUSH for better performance
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  
  // Set mutation buffer size
  KUDU_RETURN_NOT_OK(session->SetMutationBufferSpace(100 * 1024 * 1024));  // 100MB
  
  // Set timeout
  session->SetTimeoutMillis(60000);  // 60 seconds
  
  std::string line;
  int64_t rows_read = 0;
  int64_t rows_inserted = 0;
  int64_t error_count = 0;
  const int64_t kFlushInterval = 10000;  // Flush every 10k rows
  
  std::cout << "Inserting data..." << std::endl;
  
  while (std::getline(infile, line)) {
    if (line.empty()) continue;
    rows_read++;
    if (metrics && metrics->rows_read) {
      metrics->rows_read->Increment();
    }
    
    std::vector<std::string> fields = SplitByTab(line);
    
    // Expected 105 columns
    if (fields.size() != 105) {
      std::cerr << "Warning: Line " << rows_read
                << " has " << fields.size() << " fields, expected 105. Skipping." 
                << std::endl;
      error_count++;
      if (metrics && metrics->error_count) {
        metrics->error_count->Increment();
      }
      continue;
    }
    
    try {
      std::unique_ptr<KuduInsert> insert(table->NewInsert());
      KuduPartialRow* row = insert->mutable_row();
      
      // Parse and set values for all 105 columns
      // The order matches the schema definition
      KUDU_RETURN_NOT_OK(row->SetInt64("WatchID", ParseInt64(fields[0])));
      KUDU_RETURN_NOT_OK(row->SetInt16("JavaEnable", ParseInt16(fields[1])));
      KUDU_RETURN_NOT_OK(row->SetString("Title", fields[2]));
      KUDU_RETURN_NOT_OK(row->SetInt16("GoodEvent", ParseInt16(fields[3])));
      KUDU_RETURN_NOT_OK(row->SetUnixTimeMicros("EventTime", ParseInt64(fields[4])));
      KUDU_RETURN_NOT_OK(row->SetInt32("EventDate", ParseInt32(fields[5])));
      KUDU_RETURN_NOT_OK(row->SetInt32("CounterID", ParseInt32(fields[6])));
      KUDU_RETURN_NOT_OK(row->SetInt32("ClientIP", ParseInt32(fields[7])));
      KUDU_RETURN_NOT_OK(row->SetInt32("RegionID", ParseInt32(fields[8])));
      KUDU_RETURN_NOT_OK(row->SetInt64("UserID", ParseInt64(fields[9])));
      KUDU_RETURN_NOT_OK(row->SetInt16("CounterClass", ParseInt16(fields[10])));
      KUDU_RETURN_NOT_OK(row->SetInt16("OS", ParseInt16(fields[11])));
      KUDU_RETURN_NOT_OK(row->SetInt16("UserAgent", ParseInt16(fields[12])));
      KUDU_RETURN_NOT_OK(row->SetString("URL", fields[13]));
      KUDU_RETURN_NOT_OK(row->SetString("Referer", fields[14]));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsRefresh", ParseInt16(fields[15])));
      KUDU_RETURN_NOT_OK(row->SetInt16("RefererCategoryID", ParseInt16(fields[16])));
      KUDU_RETURN_NOT_OK(row->SetInt32("RefererRegionID", ParseInt32(fields[17])));
      KUDU_RETURN_NOT_OK(row->SetInt16("URLCategoryID", ParseInt16(fields[18])));
      KUDU_RETURN_NOT_OK(row->SetInt32("URLRegionID", ParseInt32(fields[19])));
      KUDU_RETURN_NOT_OK(row->SetInt16("ResolutionWidth", ParseInt16(fields[20])));
      KUDU_RETURN_NOT_OK(row->SetInt16("ResolutionHeight", ParseInt16(fields[21])));
      KUDU_RETURN_NOT_OK(row->SetInt16("ResolutionDepth", ParseInt16(fields[22])));
      KUDU_RETURN_NOT_OK(row->SetInt16("FlashMajor", ParseInt16(fields[23])));
      KUDU_RETURN_NOT_OK(row->SetInt16("FlashMinor", ParseInt16(fields[24])));
      KUDU_RETURN_NOT_OK(row->SetString("FlashMinor2", fields[25]));
      KUDU_RETURN_NOT_OK(row->SetInt16("NetMajor", ParseInt16(fields[26])));
      KUDU_RETURN_NOT_OK(row->SetInt16("NetMinor", ParseInt16(fields[27])));
      KUDU_RETURN_NOT_OK(row->SetInt16("UserAgentMajor", ParseInt16(fields[28])));
      KUDU_RETURN_NOT_OK(row->SetString("UserAgentMinor", fields[29]));
      KUDU_RETURN_NOT_OK(row->SetInt16("CookieEnable", ParseInt16(fields[30])));
      KUDU_RETURN_NOT_OK(row->SetInt16("JavascriptEnable", ParseInt16(fields[31])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsMobile", ParseInt16(fields[32])));
      KUDU_RETURN_NOT_OK(row->SetInt16("MobilePhone", ParseInt16(fields[33])));
      KUDU_RETURN_NOT_OK(row->SetString("MobilePhoneModel", fields[34]));
      KUDU_RETURN_NOT_OK(row->SetString("Params", fields[35]));
      KUDU_RETURN_NOT_OK(row->SetInt32("IPNetworkID", ParseInt32(fields[36])));
      KUDU_RETURN_NOT_OK(row->SetInt16("TraficSourceID", ParseInt16(fields[37])));
      KUDU_RETURN_NOT_OK(row->SetInt16("SearchEngineID", ParseInt16(fields[38])));
      KUDU_RETURN_NOT_OK(row->SetString("SearchPhrase", fields[39]));
      KUDU_RETURN_NOT_OK(row->SetInt16("AdvEngineID", ParseInt16(fields[40])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsArtifical", ParseInt16(fields[41])));
      KUDU_RETURN_NOT_OK(row->SetInt16("WindowClientWidth", ParseInt16(fields[42])));
      KUDU_RETURN_NOT_OK(row->SetInt16("WindowClientHeight", ParseInt16(fields[43])));
      KUDU_RETURN_NOT_OK(row->SetInt16("ClientTimeZone", ParseInt16(fields[44])));
      KUDU_RETURN_NOT_OK(row->SetUnixTimeMicros("ClientEventTime", ParseInt64(fields[45])));
      KUDU_RETURN_NOT_OK(row->SetInt16("SilverlightVersion1", ParseInt16(fields[46])));
      KUDU_RETURN_NOT_OK(row->SetInt16("SilverlightVersion2", ParseInt16(fields[47])));
      KUDU_RETURN_NOT_OK(row->SetInt32("SilverlightVersion3", ParseInt32(fields[48])));
      KUDU_RETURN_NOT_OK(row->SetInt16("SilverlightVersion4", ParseInt16(fields[49])));
      KUDU_RETURN_NOT_OK(row->SetString("PageCharset", fields[50]));
      KUDU_RETURN_NOT_OK(row->SetInt32("CodeVersion", ParseInt32(fields[51])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsLink", ParseInt16(fields[52])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsDownload", ParseInt16(fields[53])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsNotBounce", ParseInt16(fields[54])));
      KUDU_RETURN_NOT_OK(row->SetInt64("FUniqID", ParseInt64(fields[55])));
      KUDU_RETURN_NOT_OK(row->SetString("OriginalURL", fields[56]));
      KUDU_RETURN_NOT_OK(row->SetInt32("HID", ParseInt32(fields[57])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsOldCounter", ParseInt16(fields[58])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsEvent", ParseInt16(fields[59])));
      KUDU_RETURN_NOT_OK(row->SetInt16("IsParameter", ParseInt16(fields[60])));
      KUDU_RETURN_NOT_OK(row->SetInt16("DontCountHits", ParseInt16(fields[61])));
      KUDU_RETURN_NOT_OK(row->SetInt16("WithHash", ParseInt16(fields[62])));
      KUDU_RETURN_NOT_OK(row->SetString("HitColor", fields[63]));
      KUDU_RETURN_NOT_OK(row->SetUnixTimeMicros("LocalEventTime", ParseInt64(fields[64])));
      KUDU_RETURN_NOT_OK(row->SetInt16("Age", ParseInt16(fields[65])));
      KUDU_RETURN_NOT_OK(row->SetInt16("Sex", ParseInt16(fields[66])));
      KUDU_RETURN_NOT_OK(row->SetInt16("Income", ParseInt16(fields[67])));
      KUDU_RETURN_NOT_OK(row->SetInt16("Interests", ParseInt16(fields[68])));
      KUDU_RETURN_NOT_OK(row->SetInt16("Robotness", ParseInt16(fields[69])));
      KUDU_RETURN_NOT_OK(row->SetInt32("RemoteIP", ParseInt32(fields[70])));
      KUDU_RETURN_NOT_OK(row->SetInt32("WindowName", ParseInt32(fields[71])));
      KUDU_RETURN_NOT_OK(row->SetInt32("OpenerName", ParseInt32(fields[72])));
      KUDU_RETURN_NOT_OK(row->SetInt16("HistoryLength", ParseInt16(fields[73])));
      KUDU_RETURN_NOT_OK(row->SetString("BrowserLanguage", fields[74]));
      KUDU_RETURN_NOT_OK(row->SetString("BrowserCountry", fields[75]));
      KUDU_RETURN_NOT_OK(row->SetString("SocialNetwork", fields[76]));
      KUDU_RETURN_NOT_OK(row->SetString("SocialAction", fields[77]));
      KUDU_RETURN_NOT_OK(row->SetInt16("HTTPError", ParseInt16(fields[78])));
      KUDU_RETURN_NOT_OK(row->SetInt32("SendTiming", ParseInt32(fields[79])));
      KUDU_RETURN_NOT_OK(row->SetInt32("DNSTiming", ParseInt32(fields[80])));
      KUDU_RETURN_NOT_OK(row->SetInt32("ConnectTiming", ParseInt32(fields[81])));
      KUDU_RETURN_NOT_OK(row->SetInt32("ResponseStartTiming", ParseInt32(fields[82])));
      KUDU_RETURN_NOT_OK(row->SetInt32("ResponseEndTiming", ParseInt32(fields[83])));
      KUDU_RETURN_NOT_OK(row->SetInt32("FetchTiming", ParseInt32(fields[84])));
      KUDU_RETURN_NOT_OK(row->SetInt16("SocialSourceNetworkID", ParseInt16(fields[85])));
      KUDU_RETURN_NOT_OK(row->SetString("SocialSourcePage", fields[86]));
      KUDU_RETURN_NOT_OK(row->SetInt64("ParamPrice", ParseInt64(fields[87])));
      KUDU_RETURN_NOT_OK(row->SetString("ParamOrderID", fields[88]));
      KUDU_RETURN_NOT_OK(row->SetString("ParamCurrency", fields[89]));
      KUDU_RETURN_NOT_OK(row->SetInt16("ParamCurrencyID", ParseInt16(fields[90])));
      KUDU_RETURN_NOT_OK(row->SetString("OpenstatServiceName", fields[91]));
      KUDU_RETURN_NOT_OK(row->SetString("OpenstatCampaignID", fields[92]));
      KUDU_RETURN_NOT_OK(row->SetString("OpenstatAdID", fields[93]));
      KUDU_RETURN_NOT_OK(row->SetString("OpenstatSourceID", fields[94]));
      KUDU_RETURN_NOT_OK(row->SetString("UTMSource", fields[95]));
      KUDU_RETURN_NOT_OK(row->SetString("UTMMedium", fields[96]));
      KUDU_RETURN_NOT_OK(row->SetString("UTMCampaign", fields[97]));
      KUDU_RETURN_NOT_OK(row->SetString("UTMContent", fields[98]));
      KUDU_RETURN_NOT_OK(row->SetString("UTMTerm", fields[99]));
      KUDU_RETURN_NOT_OK(row->SetString("FromTag", fields[100]));
      KUDU_RETURN_NOT_OK(row->SetInt16("HasGCLID", ParseInt16(fields[101])));
      KUDU_RETURN_NOT_OK(row->SetInt64("RefererHash", ParseInt64(fields[102])));
      KUDU_RETURN_NOT_OK(row->SetInt64("URLHash", ParseInt64(fields[103])));
      KUDU_RETURN_NOT_OK(row->SetInt32("CLID", ParseInt32(fields[104])));
      
      Status s = session->Apply(insert.release());
      if (!s.ok()) {
        std::cerr << "Error applying insert for row " << rows_read << ": "
                  << s.ToString() << std::endl;
        error_count++;
        if (metrics && metrics->error_count) {
          metrics->error_count->Increment();
        }
        continue;
      }
      
      rows_inserted++;
      if (metrics && metrics->rows_inserted) {
        metrics->rows_inserted->Increment();
      }
      
      // Periodic flush for better performance
      if (rows_inserted % kFlushInterval == 0) {
        KUDU_RETURN_NOT_OK(session->Flush());
        std::cout << "Processed " << rows_inserted << " rows..." << std::endl;
      }
      
    } catch (const std::exception& e) {
      std::cerr << "Error parsing row " << rows_read << ": " << e.what() << std::endl;
      error_count++;
      if (metrics && metrics->error_count) {
        metrics->error_count->Increment();
      }
    }
  }
  
  // Final flush
  KUDU_RETURN_NOT_OK(session->Flush());
  
  infile.close();
  
  // Check for errors
  if (session->CountPendingErrors() > 0) {
    std::vector<KuduError*> errors;
    bool overflow;
    session->GetPendingErrors(&errors, &overflow);
    
    std::cerr << "\nErrors occurred during insertion:" << std::endl;
    int displayed_errors = 0;
    for (const auto* error : errors) {
      if (displayed_errors < 10) {  // Only show first 10 errors
        std::cerr << "  - " << error->status().ToString() << std::endl;
        displayed_errors++;
      }
      delete error;
    }
    
    if (errors.size() > 10) {
      std::cerr << "  ... and " << (errors.size() - 10) << " more errors" << std::endl;
    }
    
    if (overflow) {
      std::cerr << "  - Error buffer overflowed; some errors were discarded" << std::endl;
    }
  }
  
  std::cout << "\n✓ Successfully processed " << rows_inserted << " rows" << std::endl;
  if (error_count > 0) {
    std::cout << "✗ " << error_count << " rows had errors" << std::endl;
  }
  if (result != nullptr) {
    result->rows_read = rows_read;
    result->rows_inserted = rows_inserted;
    result->error_count = error_count;
  }

  return Status::OK();
}

int main(int argc, char* argv[]) {
  std::cout << std::string(80, '=') << std::endl;
  std::cout << "Kudu C++ Client - Data Insertion from TSV" << std::endl;
  std::cout << std::string(80, '=') << std::endl;
  std::cout << std::endl;
  
  AppConfig config;
  if (!ParseArgs(argc, argv, &config)) {
    PrintUsage(argv[0]);
    return 1;
  }

  auto metrics = StartMetrics(config.metrics_port);

  std::cout << "TSV File: " << config.file_path << std::endl;
  std::cout << "Connecting to Kudu master at: " << config.master_address << std::endl;
  if (metrics) {
    std::cout << "Metrics endpoint: http://localhost:" << config.metrics_port
              << "/metrics" << std::endl;
  } else {
    std::cout << "Metrics endpoint: disabled" << std::endl;
  }
  
  // Create Kudu client
  shared_ptr<KuduClient> client;
  Status s = KuduClientBuilder()
    .add_master_server_addr(config.master_address)
    .Build(&client);
  
  if (!s.ok()) {
    std::cerr << "Error creating Kudu client: " << s.ToString() << std::endl;
    return 1;
  }
  
  std::cout << "✓ Connected to Kudu master" << std::endl;
  std::cout << std::endl;
  
  // Create the table
  s = CreateTable(client);
  if (!s.ok()) {
    std::cerr << "Error creating table: " << s.ToString() << std::endl;
    return 1;
  }
  
  // Start timer
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Insert data from file
  IngestResult ingest_result;
  s = InsertDataFromFile(client, config.file_path, metrics.get(), &ingest_result);
  if (!s.ok()) {
    std::cerr << "Error inserting data: " << s.ToString() << std::endl;
    return 1;
  }
  
  // Stop timer and calculate duration
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  double seconds = duration.count() / 1000.0;
  double rows_per_second = seconds > 0.0
      ? static_cast<double>(ingest_result.rows_inserted) / seconds
      : 0.0;
  if (metrics) {
    metrics->elapsed_seconds->Set(seconds);
    metrics->rows_per_second->Set(rows_per_second);
  }
  WriteMetricsSummary(config.metrics_output_path, ingest_result, seconds,
                      rows_per_second);
  
  std::cout << std::endl;
  std::cout << std::string(80, '=') << std::endl;
  std::cout << "✓ Data ingestion completed successfully!" << std::endl;
  std::cout << "Time taken: " << seconds << " seconds" << std::endl;
  std::cout << std::string(80, '=') << std::endl;
  std::cout << std::endl;

  KeepMetricsAlive(config);

  return 0;
}
