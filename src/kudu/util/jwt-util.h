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
//
// Copied from Impala and adapted to Kudu.
#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "kudu/util/jwt.h"
#include "kudu/util/logging.h"
#include "kudu/util/promise.h"
#include "kudu/util/status.h"

namespace kudu {

class JWKSSnapshot;
class JWKSMgr;

// JSON Web Token (JWT) is an Internet proposed standard for creating data with optional
// signature and/or optional encryption whose payload holds JSON that asserts some
// number of claims. The tokens are signed either using a private secret or a public/
// private key.
// This class works as wrapper for jwt-cpp. It provides APIs to decode/verify JWT token,
// and extracts custom claim from the payload of JWT token.
// The class is thread safe.
class JWTHelper {
 public:
  // Opaque types for storing the JWT decoded token. This allows us to avoid including
  // header file jwt-cpp/jwt.h.
  struct JWTDecodedToken;

  // Custom deleter: intended for use with std::unique_ptr<JWTDecodedToken>.
  class TokenDeleter {
   public:
    // Called by unique_ptr to free JWTDecodedToken
    void operator()(JWTHelper::JWTDecodedToken* token) const;
  };
  // UniqueJWTDecodedToken -- a wrapper around opaque decoded token structure to
  // facilitate automatic reference counting.
  typedef std::unique_ptr<JWTDecodedToken, TokenDeleter> UniqueJWTDecodedToken;

  // Define our destructor elsewhere so JWKSMgr's destructor is called from the
  // correct compilation unit.
  ~JWTHelper();

  // Return the single instance.
  static JWTHelper* GetInstance() { return jwt_helper_; }

  // Load JWKS from a given local JSON file or URL. Returns an error if problems were
  // encountered.
  Status Init(const std::string& jwks_uri, bool is_local_file);

  // Decode the given JWT token. The decoding result is stored in decoded_token_.
  // Return Status::OK if the decoding is successful.
  static Status Decode(
      const std::string& token, UniqueJWTDecodedToken& decoded_token_out);

  // Verify the token's signature with the JWKS. The token should be already decoded by
  // calling Decode().
  // Return Status::OK if the verification is successful.
  Status Verify(const JWTDecodedToken* decoded_token) const;

  // Extract custom claim "Username" from from the payload of the decoded JWT token.
  // Return Status::OK if the extraction is successful.
  static Status GetCustomClaimUsername(const JWTDecodedToken* decoded_token,
      const std::string& custom_claim_username, std::string& username);

  // Return snapshot of JWKS.
  std::shared_ptr<const JWKSSnapshot> GetJWKS() const;

 private:
  // Single instance.
  static JWTHelper* jwt_helper_;

  // Set it as TRUE when Init() is called.
  bool initialized_ = false;

  // JWKS Manager for Json Web Token (JWT) verification.
  // Only one instance per helper.
  std::unique_ptr<JWKSMgr> jwks_mgr_;
};

// JwtVerifier implementation that uses JWKS to verify tokens.
class KeyBasedJwtVerifier : public JwtVerifier {
 public:
  KeyBasedJwtVerifier(const std::string& jwks_uri, bool is_local_file)
      : jwt_(JWTHelper::GetInstance()) {
    CHECK_OK(jwt_->Init(jwks_uri, is_local_file));
  }
  ~KeyBasedJwtVerifier() = default;
  Status VerifyToken(const std::string& bytes_raw, std::string* subject) override;
 private:
  JWTHelper* jwt_;

  DISALLOW_COPY_AND_ASSIGN(KeyBasedJwtVerifier);
};

class PerAccountKeyBasedJwtVerifier : public JwtVerifier {
 public:
  PerAccountKeyBasedJwtVerifier(const std::string& oidc_uri)
      : oidc_uri_(oidc_uri) {}
  ~PerAccountKeyBasedJwtVerifier() = default;
  Status VerifyToken(const std::string& bytes_raw, std::string* subject) override;
 private:
  // Gets the appropriate JWTHelper, based on the token and source mode.
  // Returns an error if the token doesn't contain the appropriate fields.
  Status JWTHelperForToken(const JWTHelper::JWTDecodedToken& token, JWTHelper** helper);

  std::string oidc_uri_;
  std::unordered_map<std::string, std::unique_ptr<JWTHelper>> jwt_by_account_id_;

  DISALLOW_COPY_AND_ASSIGN(PerAccountKeyBasedJwtVerifier);
};

} // namespace kudu
