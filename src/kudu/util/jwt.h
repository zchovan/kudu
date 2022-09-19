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
#pragma once

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

// Minimal header to avoid having to link heavy-weight modules into libraries.

namespace kudu {

class JwtVerifier {
 public:
  virtual ~JwtVerifier() {}
  virtual Status VerifyToken(const std::string& bytes_raw, std::string* subject) = 0;
};

// Minimal implementation of a JWT verifier to be used when a more full-fledged
// implementation is not available.
class SimpleJwtVerifier : public JwtVerifier {
 public:
  SimpleJwtVerifier() = default;
  ~SimpleJwtVerifier() = default;
  Status VerifyToken(const std::string& bytes_raw, std::string* subject) override {
    return Status::NotAuthorized("JWT verification not configured");
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(SimpleJwtVerifier);
};

} // namespace kudu
