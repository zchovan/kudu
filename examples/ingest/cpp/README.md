# Kudu C++ Client - TSV Data Ingestion

This is a high-performance C++ program that uses the native Apache Kudu C++ client to create a table and ingest data from a TSV (Tab-Separated Values) file. The example is based on the official Apache Kudu C++ client examples.

## Features

- Connects to a local Kudu cluster
- Creates a table with a predefined schema (if not exists)
- Reads data from TSV file
- Batch inserts with manual flush control for high performance
- Configurable flush interval, mutation buffer size, and writer threads
- Multi-threaded ingestion by splitting the input file
- Dry-run mode for parse-only benchmarking
- JSON summary output (including per-flush latencies)
- Performance timing and metrics
- Comprehensive error handling and reporting
- Supports large datasets with efficient buffering

## Table Schema

**Table Name:** `default.hits`

This table uses the ClickHouse hits benchmark schema with 105 columns for web analytics data.

**Primary Key Columns (in order):**
1. CounterID (INT32)
2. EventDate (INT32)
3. UserID (INT64)
4. EventTime (UNIXTIME_MICROS)
5. WatchID (INT64)

**Partitioning:** 
- Hash partitioning on (CounterID, EventDate, UserID) with 8 buckets
- Range partitioning on EventDate

**Complete Schema (105 columns):**

| # | Column Name | Type | Nullable | Description |
|---|-------------|------|----------|-------------|
| 1 | WatchID | INT64 | No | Primary key component |
| 2 | JavaEnable | INT16 | No | Java enabled flag |
| 3 | Title | STRING | No | Page title |
| 4 | GoodEvent | INT16 | No | Good event indicator |
| 5 | EventTime | UNIXTIME_MICROS | No | Primary key component |
| 6 | EventDate | INT32 | No | Primary key component |
| 7 | CounterID | INT32 | No | Primary key component |
| 8 | ClientIP | INT32 | No | Client IP address |
| 9 | RegionID | INT32 | No | Region identifier |
| 10 | UserID | INT64 | No | Primary key component |
| 11 | CounterClass | INT16 | No | Counter class |
| 12 | OS | INT16 | No | Operating system |
| 13 | UserAgent | INT16 | No | User agent |
| 14 | URL | STRING | No | Page URL |
| 15 | Referer | STRING | No | Referrer URL |
| 16 | IsRefresh | INT16 | No | Refresh indicator |
| 17 | RefererCategoryID | INT16 | No | Referer category |
| 18 | RefererRegionID | INT32 | No | Referer region |
| 19 | URLCategoryID | INT16 | No | URL category |
| 20 | URLRegionID | INT32 | No | URL region |
| 21 | ResolutionWidth | INT16 | No | Screen width |
| 22 | ResolutionHeight | INT16 | No | Screen height |
| 23 | ResolutionDepth | INT16 | No | Color depth |
| 24 | FlashMajor | INT16 | No | Flash major version |
| 25 | FlashMinor | INT16 | No | Flash minor version |
| 26 | FlashMinor2 | STRING | No | Flash minor version 2 |
| 27 | NetMajor | INT16 | No | .NET major version |
| 28 | NetMinor | INT16 | No | .NET minor version |
| 29 | UserAgentMajor | INT16 | No | User agent major version |
| 30 | UserAgentMinor | STRING | No | User agent minor version |
| 31 | CookieEnable | INT16 | No | Cookie enabled |
| 32 | JavascriptEnable | INT16 | No | JavaScript enabled |
| 33 | IsMobile | INT16 | No | Mobile device flag |
| 34 | MobilePhone | INT16 | No | Mobile phone type |
| 35 | MobilePhoneModel | STRING | No | Mobile phone model |
| 36 | Params | STRING | No | URL parameters |
| 37 | IPNetworkID | INT32 | No | IP network ID |
| 38 | TraficSourceID | INT16 | No | Traffic source |
| 39 | SearchEngineID | INT16 | No | Search engine |
| 40 | SearchPhrase | STRING | No | Search phrase |
| 41 | AdvEngineID | INT16 | No | Ad engine |
| 42 | IsArtifical | INT16 | No | Artificial traffic flag |
| 43 | WindowClientWidth | INT16 | No | Window width |
| 44 | WindowClientHeight | INT16 | No | Window height |
| 45 | ClientTimeZone | INT16 | No | Client timezone |
| 46 | ClientEventTime | UNIXTIME_MICROS | No | Client event time |
| 47 | SilverlightVersion1 | INT16 | No | Silverlight version 1 |
| 48 | SilverlightVersion2 | INT16 | No | Silverlight version 2 |
| 49 | SilverlightVersion3 | INT32 | No | Silverlight version 3 |
| 50 | SilverlightVersion4 | INT16 | No | Silverlight version 4 |
| 51 | PageCharset | STRING | No | Page charset |
| 52 | CodeVersion | INT32 | No | Code version |
| 53 | IsLink | INT16 | No | Link indicator |
| 54 | IsDownload | INT16 | No | Download indicator |
| 55 | IsNotBounce | INT16 | No | Non-bounce indicator |
| 56 | FUniqID | INT64 | No | Unique function ID |
| 57 | OriginalURL | STRING | No | Original URL |
| 58 | HID | INT32 | No | Hit ID |
| 59 | IsOldCounter | INT16 | No | Old counter flag |
| 60 | IsEvent | INT16 | No | Event flag |
| 61 | IsParameter | INT16 | No | Parameter flag |
| 62 | DontCountHits | INT16 | No | Don't count hits flag |
| 63 | WithHash | INT16 | No | With hash flag |
| 64 | HitColor | STRING | No | Hit color |
| 65 | LocalEventTime | UNIXTIME_MICROS | No | Local event time |
| 66 | Age | INT16 | No | User age |
| 67 | Sex | INT16 | No | User gender |
| 68 | Income | INT16 | No | Income level |
| 69 | Interests | INT16 | No | User interests |
| 70 | Robotness | INT16 | No | Robot indicator |
| 71 | RemoteIP | INT32 | No | Remote IP |
| 72 | WindowName | INT32 | No | Window name hash |
| 73 | OpenerName | INT32 | No | Opener name hash |
| 74 | HistoryLength | INT16 | No | Browser history length |
| 75 | BrowserLanguage | STRING | No | Browser language |
| 76 | BrowserCountry | STRING | No | Browser country |
| 77 | SocialNetwork | STRING | No | Social network |
| 78 | SocialAction | STRING | No | Social action |
| 79 | HTTPError | INT16 | No | HTTP error code |
| 80 | SendTiming | INT32 | No | Send timing |
| 81 | DNSTiming | INT32 | No | DNS timing |
| 82 | ConnectTiming | INT32 | No | Connect timing |
| 83 | ResponseStartTiming | INT32 | No | Response start timing |
| 84 | ResponseEndTiming | INT32 | No | Response end timing |
| 85 | FetchTiming | INT32 | No | Fetch timing |
| 86 | SocialSourceNetworkID | INT16 | No | Social source network |
| 87 | SocialSourcePage | STRING | No | Social source page |
| 88 | ParamPrice | INT64 | No | Parameter price |
| 89 | ParamOrderID | STRING | No | Parameter order ID |
| 90 | ParamCurrency | STRING | No | Parameter currency |
| 91 | ParamCurrencyID | INT16 | No | Parameter currency ID |
| 92 | OpenstatServiceName | STRING | No | Openstat service |
| 93 | OpenstatCampaignID | STRING | No | Openstat campaign |
| 94 | OpenstatAdID | STRING | No | Openstat ad |
| 95 | OpenstatSourceID | STRING | No | Openstat source |
| 96 | UTMSource | STRING | No | UTM source |
| 97 | UTMMedium | STRING | No | UTM medium |
| 98 | UTMCampaign | STRING | No | UTM campaign |
| 99 | UTMContent | STRING | No | UTM content |
| 100 | UTMTerm | STRING | No | UTM term |
| 101 | FromTag | STRING | No | From tag |
| 102 | HasGCLID | INT16 | No | Has GCLID flag |
| 103 | RefererHash | INT64 | No | Referer hash |
| 104 | URLHash | INT64 | No | URL hash |
| 105 | CLID | INT32 | No | Client ID |

**TSV Column Order:**
The TSV file must have columns in this exact order: WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate, CounterID, ClientIP, RegionID, UserID, ... CLID (105 total columns).

## Prerequisites

### 1. Kudu Installation

You need the Kudu C++ client library installed. Options:

**Option A: Build from source**
```bash
# Clone Kudu repository
git clone https://github.com/apache/kudu.git
cd kudu

# Build Kudu (this will take a while)
./build-support/enable_devtoolset.sh thirdparty/build-if-necessary.sh
mkdir -p build/release
cd build/release
../../build-support/enable_devtoolset.sh \
  ../../thirdparty/installed/common/bin/cmake \
  -DCMAKE_BUILD_TYPE=release ../..
make -j$(nproc)
sudo make install
```

**Option B: Install pre-built packages (if available for your OS)**
```bash
# Ubuntu/Debian
sudo apt-get install kudu-client-dev

# CentOS/RHEL
sudo yum install kudu-client-devel
```

**Option C: Use Docker**
```bash
# Use the official Kudu docker image which includes the client libraries
docker pull apache/kudu
```
Note: the provided `cpp/Dockerfile` uses `apache/kudu:kudu-python-1.18.1-ubuntu` as its base image
because it already bundles the Kudu client libraries.

### 2. Build Tools

- CMake 3.11 or later
- C++11 compatible compiler (GCC 4.8+, Clang 3.4+)

### 3. Running Kudu Cluster

You need a Kudu cluster running locally. The easiest way:

```bash
# Pull and run Kudu quickstart
docker run -d --name kudu-master-data apache/kudu master
docker run -d --name kudu-tserver-data apache/kudu tserver
```

Or use the Kudu quickstart VM.

## Building

### Set KUDU_HOME (if Kudu is not in standard locations)

```bash
export KUDU_HOME=/path/to/kudu/installation
```

### Build the project

```bash
cd cpp
mkdir build
cd build
cmake ..
make
```

This will create the `kudu_insert` executable.

### Build with Makefile

```bash
cd cpp
make build
```

## Running

### Basic usage:

```bash
./kudu_insert <path_to_tsv_file> [master_address]
./kudu_insert <path_to_tsv_file> --master <host:port> --metrics-port <port>
./kudu_insert <path_to_tsv_file> --flush-interval 10000 --buffer-size-mb 100 \
  --num-threads 4
./kudu_insert <path_to_tsv_file> --dry-run --num-threads 4
```

### Examples:

```bash
# Using default master address (localhost:7051)
./kudu_insert data.tsv

# Specifying custom master address
./kudu_insert data.tsv 192.168.1.100:7051

# Using flags for master address and metrics port
./kudu_insert data.tsv --master 192.168.1.100:7051 --metrics-port 8080

# With absolute path
./kudu_insert /path/to/your/data.tsv
```

If `--master` is not supplied, the client uses `KUDU_MASTER` (if set) or
`localhost:7051`.

### Tuning flags

- `--flush-interval <rows>` controls how many rows each thread buffers before a
  manual flush (default: 10000).
- `--buffer-size-mb <mb>` sets the mutation buffer size per session (default: 100).
- `--num-threads <n>` splits the input file into N segments and runs N parallel
  writer threads (default: 1).
- `--dry-run` parses the TSV without writing to Kudu to isolate I/O costs.

Suggested sweep values:

- Writer threads: 1, 4, 16, 32
- Flush interval: 1k, 10k, 50k rows
- Mutation buffer size: 50MB, 100MB, 500MB

### Metrics endpoint

By default, the ingest process exposes Prometheus metrics at
`http://localhost:8080/metrics`. Override the port with `--metrics-port` or
`KUDU_INGEST_METRICS_PORT`. Set the port to `0` to disable metrics.

To persist a metrics summary, use `--metrics-output /path/to/metrics.json` (or
`KUDU_INGEST_METRICS_OUTPUT`). To keep the metrics endpoint alive after ingest,
use `--metrics-keepalive-seconds <n>` or `--metrics-keepalive`.

To push metrics to a Prometheus Pushgateway, set `--metrics-push-gateway
<host:port>` (or `KUDU_INGEST_METRICS_PUSH_GATEWAY`). Override the job name with
`--metrics-push-job`.

## TSV File Format

The TSV file should have:
- Tab-separated values (no header row)
- 105 columns per row matching the schema order
- Values in the exact order described in the schema table above

See the table schema section in this document for the exact column order.

## Expected Output

```
================================================================================
Kudu C++ Client - Data Insertion from TSV
================================================================================

TSV File: data.tsv
Connecting to Kudu master at: localhost:7051
✓ Connected to Kudu master

Creating table 'default.hits'...
Table 'default.hits' created successfully

Reading data from file: data.tsv
Inserting data...
Processed 10000 rows...
Processed 20000 rows...
Processed 30000 rows...

✓ Successfully processed 30000 rows

================================================================================
✓ Data ingestion completed successfully!
Time taken: 12.345 seconds
================================================================================
```

After the human-readable summary, the program emits a JSON result line to
stdout that includes `rows_inserted`, `elapsed_seconds`, `rows_per_second`,
`error_count`, and `flush_latencies_ms` for automated collection.

## Troubleshooting

### CMake cannot find Kudu

If CMake cannot find the Kudu library:

1. Set `KUDU_HOME` environment variable:
   ```bash
   export KUDU_HOME=/usr/local
   ```

2. Or specify the path explicitly in CMake:
   ```bash
   cmake -DKUDU_HOME=/path/to/kudu ..
   ```

### Linking errors

If you get linking errors, you may need to add the library path:

```bash
export LD_LIBRARY_PATH=$KUDU_HOME/lib:$LD_LIBRARY_PATH
```

Or on macOS:
```bash
export DYLD_LIBRARY_PATH=$KUDU_HOME/lib:$DYLD_LIBRARY_PATH
```

### Connection refused

If you get "Connection refused" errors:

1. Ensure Kudu is running:
   ```bash
   # Check if Kudu master is listening
   netstat -tuln | grep 7051
   
   # Or check Kudu processes
   ps aux | grep kudu
   ```

2. Verify the master address is correct

3. Check firewall settings

### Table already exists with different schema

If the table exists with a different schema, drop it first:

```bash
kudu table delete localhost:7051 default.hits
```

### File format errors

If you see "expected 105 fields" errors, verify:
1. Your TSV file has exactly 105 tab-separated columns
2. There are no extra tabs or missing fields
3. The file uses tab characters (\t), not spaces

## Code Structure

The program follows this flow:

1. **Parse Arguments** - Validates TSV file path and master address
2. **Connect to Kudu** - Establishes connection to the master server
3. **Create Table** - Defines schema and creates table with hash/range partitioning
4. **Read TSV File** - Opens and parses the input file line by line
5. **Insert Data** - Uses KuduSession with manual flush for batch insertion
6. **Error Handling** - Tracks and reports parsing/insertion errors
7. **Performance Timing** - Measures and reports total ingestion time

## Key Kudu C++ Client Concepts

### KuduClient
The main entry point for interacting with Kudu. Created using `KuduClientBuilder`.

### KuduSession
Used to apply write operations (INSERT, UPDATE, DELETE). Supports different flush modes:
- `AUTO_FLUSH_SYNC` - Flushes after each operation (low performance)
- `AUTO_FLUSH_BACKGROUND` - Asynchronous batching
- `MANUAL_FLUSH` - Manual control over flushing (used in this example for best performance)

### Status
Kudu's error handling mechanism. Always check Status objects with `.ok()` and handle errors appropriately.

## Performance Tuning

The program is optimized for high-throughput ingestion:

1. **Manual Flush Mode** - Batches operations instead of flushing each insert
2. **Large Mutation Buffer** - 100MB buffer for better batching
3. **Periodic Flush** - Flushes every 10,000 rows for progress tracking
4. **Efficient Parsing** - Direct string-to-type conversion without intermediate copies

For even better performance on large datasets:
- Increase `--flush-interval` for less frequent flushes
- Increase `--buffer-size-mb` if you have more RAM
- Use `--num-threads` to parallelize ingestion

## Further Reading

- [Apache Kudu C++ Client API Documentation](https://kudu.apache.org/cpp-client-api/)
- [Apache Kudu Documentation](https://kudu.apache.org/docs/)
- [Kudu C++ Examples](https://github.com/apache/kudu/tree/master/examples/cpp)

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
