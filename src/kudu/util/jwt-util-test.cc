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

#include <cstdio>
#include <memory>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/util/jwt.h"
#include "kudu/util/jwt_test_certs.h"
#include "kudu/util/jwt-util-internal.h"
#include "kudu/util/jwt-util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

/// Utility class for creating a file that will be automatically deleted upon test
/// completion.
class TempTestDataFile {
 public:
  // Creates a temporary file with the specified contents.
  TempTestDataFile(const std::string& contents);

  ~TempTestDataFile() { Delete(); }

  /// Returns the absolute path to the file.
  const std::string& Filename() const { return name_; }

 private:
  std::string name_;
  bool deleted_;

  // Delete this temporary file
  void Delete();
};

TempTestDataFile::TempTestDataFile(const std::string& contents)
  : name_("/tmp/jwks_XXXXXX"), deleted_(false) {
  int fd = mkstemp(&name_[0]);
  if (fd == -1) {
    std::cout << "Error creating temp file; " << strerror(errno) << std::endl;
    abort();
  }
  if (close(fd) != 0) {
    std::cout << "Error closing temp file; " << strerror(errno) << std::endl;
    abort();
  }

  FILE* handle = fopen(name_.c_str(), "w");
  if (handle == nullptr) {
    std::cout << "Error creating temp file; " << strerror(errno) << std::endl;
    abort();
  }
  int status = fputs(contents.c_str(), handle);
  if (status < 0) {
    std::cout << "Error writing to temp file; " << strerror(errno) << std::endl;
    abort();
  }
  status = fclose(handle);
  if (status != 0) {
    std::cout << "Error closing temp file; " << strerror(errno) << std::endl;
    abort();
  }
}

void TempTestDataFile::Delete() {
  if (deleted_) return;
  deleted_ = true;
  if (remove(name_.c_str()) != 0) {
    std::cout << "Error deleting temp file; " << strerror(errno) << std::endl;
    abort();
  }
}

TEST(JwtUtilTest, LoadJwksFile) {
  // Load JWKS from file.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_FALSE(jwks->IsEmpty());
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ("rs256", key1->get_algorithm());
  ASSERT_EQ(rsa_pub_key_pem, key1->get_key());

  std::string non_existing_kid("public:c424b67b-fe28-45d7-b015-f79da5-xxxxx");
  const JWTPublicKey* key3 = jwks->LookupRSAPublicKey(non_existing_kid);
  ASSERT_FALSE(key3 != nullptr);
}

TEST(JwtUtilTest, LoadInvalidJwksFiles) {
  // JWK without kid.
  std::unique_ptr<TempTestDataFile> jwks_file(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "    }"
      "  ]"
      "}"));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("parsing key #0") != std::string::npos)
      << " Actual error: " << status.ToString();
  ASSERT_TRUE(status.ToString().find("'kid' property is required") != std::string::npos)
      << "actual error: " << status.ToString();

  // Invalid JSON format, missing "]" and "}".
  jwks_file.reset(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"kid\": \"public:c424b67b-fe28-45d7-b015-f79da50b5b21\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "}"));
  status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("Missing a comma or ']' after an array element")
      != std::string::npos)
      << " Actual error: " << status.ToString();

  // JWKS with empty key id.
  jwks_file.reset(new TempTestDataFile(
      Substitute(jwks_rsa_file_format, "", "RS256", rsa_pub_key_jwk_n, rsa_pub_key_jwk_e,
          "", "RS256", rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e)));
  status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("parsing key #0") != std::string::npos)
      << " Actual error: " << status.ToString();
  ASSERT_TRUE(status.ToString().find("'kid' property must be a non-empty string")
      != std::string::npos)
      << " Actual error: " << status.ToString();

  // JWKS with empty key value.
  jwks_file.reset(new TempTestDataFile(
      Substitute(jwks_rsa_file_format, kid_1, "RS256", "", "", kid_2, "RS256", "", "")));
  status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("parsing key #0") != std::string::npos)
      << " Actual error: " << status.ToString();
  ASSERT_TRUE(status.ToString().find("'n' and 'e' properties must be a non-empty string")
      != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtHS256) {
  // Cryptographic algorithm: HS256.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "Yx57JSBzhGFDgDj19CabRpH/+kiaKqI6UZI6lDunQKw=";
  TempTestDataFile jwks_file(
      Substitute(jwks_hs_file_format, kid_1, "HS256", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS256")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs256(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtHS384) {
  // Cryptographic algorithm: HS384.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret =
      "TlqmKRc2PNQJXTC3Go7eAadwPxA7x9byyXCi5I8tSvxrE77tYbuF5pfZAyswrkou";
  TempTestDataFile jwks_file(
      Substitute(jwks_hs_file_format, kid_1, "HS384", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS384")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs384(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtHS512) {
  // Cryptographic algorithm: HS512.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "ywc6DN7+iRw1E5HOqzvrsYodykSLFutT28KN3bJnLZcZpPCNjn0b6gbMfXPcxeY"
                         "VyuWWGDxh6gCDwPMejbuEEg==";
  TempTestDataFile jwks_file(
      Substitute(jwks_hs_file_format, kid_1, "HS512", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS512")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs512(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS256) {
  // Cryptographic algorithm: RS256.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYj"
      "ViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoiaW1wYWxhIn0.OW5H2SClL"
      "lsotsCarTHYEbqlbRh43LFwOyo9WubpNTwE7hTuJDsnFoVrvHiWI02W69TZNat7DYcC86A_ogLMfNXagHj"
      "lMFJaRnvG5Ekag8NRuZNJmHVqfX-qr6x7_8mpOdU554kc200pqbpYLhhuK4Qf7oT7y9mOrtNrUKGDCZ0Q2"
      "y_mizlbY6SMg4RWqSz0RQwJbRgXIWSgcbZd0GbD_MQQ8x7WRE4nluU-5Fl4N2Wo8T9fNTuxALPiuVeIczO"
      "25b5n4fryfKasSgaZfmk0CoOJzqbtmQxqiK9QNSJAiH2kaqMwLNgAdgn8fbd-lB1RAEGeyPH8Px8ipqcKs"
      "Pk0bg",
      token);

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier = jwt::verify()
                      .allow_algorithm(jwt::algorithm::rs256(rsa_pub_key_pem, "", "", ""))
                      .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS384) {
  // Cryptographic algorithm: RS384.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS384",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS384", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with RS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS384")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs384(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS512) {
  // Cryptographic algorithm: RS512.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS512",
      rsa512_pub_key_jwk_n, rsa512_pub_key_jwk_e, kid_2, "RS512",
      rsa512_invalid_pub_key_jwk_n, rsa512_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa512_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with RS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs512(rsa512_pub_key_pem, rsa512_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtPS256) {
  // Cryptographic algorithm: PS256.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "PS256",
      rsa1024_pub_key_jwk_n, rsa1024_pub_key_jwk_e, kid_2, "PS256",
      rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa1024_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with PS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS256")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::ps256(rsa1024_pub_key_pem, rsa1024_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtPS384) {
  // Cryptographic algorithm: PS384.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "PS384",
      rsa2048_pub_key_jwk_n, rsa2048_pub_key_jwk_e, kid_2, "PS384",
      rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa2048_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with PS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS384")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::ps384(rsa2048_pub_key_pem, rsa2048_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtPS512) {
  // Cryptographic algorithm: PS512.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "PS512",
      rsa4096_pub_key_jwk_n, rsa4096_pub_key_jwk_e, kid_2, "PS512",
      rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa4096_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with PS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS512")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::ps512(rsa4096_pub_key_pem, rsa4096_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtES256) {
  // Cryptographic algorithm: ES256.
  TempTestDataFile jwks_file(Substitute(jwks_ec_file_format, kid_1, "P-256",
      ecdsa256_pub_key_jwk_x, ecdsa256_pub_key_jwk_y));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(ecdsa256_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with ES256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES256")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::es256(
                       ecdsa256_pub_key_pem, ecdsa256_priv_key_pem, "", ""));

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier =
      jwt::verify()
          .allow_algorithm(jwt::algorithm::es256(ecdsa256_pub_key_pem, "", "", ""))
          .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtES384) {
  // Cryptographic algorithm: ES384.
  TempTestDataFile jwks_file(Substitute(jwks_ec_file_format, kid_1, "P-384",
      ecdsa384_pub_key_jwk_x, ecdsa384_pub_key_jwk_y));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(ecdsa384_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with ES384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES384")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::es384(
                       ecdsa384_pub_key_pem, ecdsa384_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtES512) {
  // Cryptographic algorithm: ES512.
  TempTestDataFile jwks_file(Substitute(jwks_ec_file_format, kid_1, "P-521",
      ecdsa521_pub_key_jwk_x, ecdsa521_pub_key_jwk_y));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(ecdsa521_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with ES512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES512")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::es512(
                       ecdsa521_pub_key_pem, ecdsa521_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtNotVerifySignature) {
  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));

  // Do not verify signature.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  Status status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtFailMismatchingAlgorithms) {
  // JWT algorithm is not matching with algorithm in JWK.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token, but set mismatching algorithm.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kid_1)
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  // Failed to verify the token due to mismatching algorithms.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find(
                  "JWT algorithm 'rs512' is not matching with JWK algorithm 'rs256'")
      != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtFailKeyNotFound) {
  // The key cannot be found in JWKS.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token with a key ID which can not be found in JWKS.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id("unfound-key-id")
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  // Failed to verify the token since key is not found in JWKS.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(
      status.ToString().find("Invalid JWK ID in the JWT token") != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").set_algorithm("RS256").sign(
          jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  // Verify the token by trying each key in JWK set and there is one matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").set_algorithm("RS512").sign(
          jwt::algorithm::rs512(rsa512_pub_key_pem, rsa512_priv_key_pem, "", ""));
  // Verify the token by trying each key in JWK set, but there is no matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutSignature) {
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token without signature.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").sign(jwt::algorithm::none{});
  // Failed to verify the unsigned token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("Unsecured JWT") != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtFailExpiredToken) {
  // Sign JWT token with RS256.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kid_1)
          .set_issued_at(std::chrono::system_clock::now())
          .set_expires_at(std::chrono::system_clock::now() - std::chrono::seconds{10})
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));

  // Verify the token, including expiring time.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("Verification failed, error: token expired")
      != std::string::npos)
      << " Actual error: " << status.ToString();
}

namespace {

// Returns a simple JWKS to be used by tokens signed by 'rsa_pub_key_pem' and
// 'rsa_priv_key_pem'.
void SimpleJWKSHandler(const Webserver::WebRequest& /*req*/,
                       Webserver::PrerenderedWebResponse* resp) {
  resp->output <<
      Substitute(jwks_rsa_file_format, kid_1, "RS256",
          rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
          rsa_pub_key_jwk_e);
  resp->status_code = HttpStatusCode::Ok;
}

class JWKSMockServer {
 public:
  // Registers a path handler for a single JWKS to be used by tokens signed by
  // 'rsa_pub_key_pem' and 'rsa_priv_key_pem'.
  Status Start() {
    DCHECK(!webserver_);
    WebserverOptions opts;
    opts.port = 0;
    webserver_.reset(new Webserver(std::move(opts)));
    webserver_->RegisterPrerenderedPathHandler("/jwks", "JWKS", SimpleJWKSHandler,
                                               /*is_styled*/false, /*is_on_nav_bar*/false);
    RETURN_NOT_OK(webserver_->Start());
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(webserver_->GetBoundAddresses(&addrs));
    RETURN_NOT_OK(addr_.ParseString("127.0.0.1", addrs[0].port()));
    url_ = Substitute("http://$0/jwks", addr_.ToString());
    return Status::OK();
  }

  // Register a path handler for every account ID in 'account_id_to_resp' that
  // returns the correspodning HTTP response.
  Status StartWithAccounts(const unordered_map<string, string>& account_id_to_resp) {
    DCHECK(!webserver_);
    WebserverOptions opts;
    opts.port = 0;
    webserver_.reset(new Webserver(std::move(opts)));
    for (const auto& ar : account_id_to_resp) {
      const auto& account_id = ar.first;
      const auto& jwks = ar.second;
      webserver_->RegisterPrerenderedPathHandler(Substitute("/jwks/$0", account_id), account_id,
          [account_id, jwks] (const Webserver::WebRequest& req,
                              Webserver::PrerenderedWebResponse* resp) {
            resp->output << jwks;
            resp->status_code = HttpStatusCode::Ok;
          },
          /*is_styled*/false, /*is_on_nav_bar*/false);
    }
    RETURN_NOT_OK(webserver_->Start());
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(webserver_->GetBoundAddresses(&addrs));
    RETURN_NOT_OK(addr_.ParseString("127.0.0.1", addrs[0].port()));
    url_ = Substitute("http://$0/jwks", addr_.ToString());
    return Status::OK();
  }

  const string& url() const {
    return url_;
  }

  string url_for_account(const string& account_id) const {
    return Substitute("$0/$1", url_, account_id);
  }
 private:
  unique_ptr<Webserver> webserver_;

  string url_;
  Sockaddr addr_;
};

} // anonymous namespace

TEST(JwtUtilTest, VerifyJWKSUrl) {
  JWKSMockServer jwks_server;
  ASSERT_OK(jwks_server.Start());

  auto encoded_token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWT")
          .set_algorithm("RS256")
          .set_key_id(kid_1)
          .set_subject("impala")
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYj"
      "ViMjEiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJhdXRoMCIsInN1YiI6ImltcGFsYSJ9.Y3GsfMBjeEukmPhF"
      "magmypjZDta0jtrIOqXKjxZafc0fzZXa5qqxq-AbV9tf02D6JmiU_YTFcaQFNpnqnL51rxXXJ75ezkiHLO"
      "rCwQC2ST_xPp_ptltT-gKYtT5c_VTt4ISQ3yKH_ND_3tHH7ujSc68NoQ2bh8XvF70lcsM-qFm-IvHL0z-0"
      "qQmKTMKhaxhFMBivb0-Mrevh5vefHqDReN9LOVWVJmYaWNbBUY6aYAN6r8fJUpXiwDdfkJ7iGBI9KE2Yyc"
      "mTPQBYPMbSFND0DcPVeOrOGXli1Txb1HwXrPmm-GnzYG2PZI13KoqkKBe5Uz3X_x1mI7Sqw-51zUYIzw",
      encoded_token);
  KeyBasedJwtVerifier jwt_verifier(jwks_server.url(), /*is_local_file*/false);
  string subject;
  ASSERT_OK(jwt_verifier.VerifyToken(encoded_token, &subject));
  ASSERT_EQ("impala", subject);
}

namespace {

// $0: account_id
// $1: jwks_uri
const string kDiscoveryFormat = R"({
    "issuer": "auth0/$0",
    "token_endpoint": "dummy.endpoint.com",
    "response_types_supported": [
        "id_token"
    ],
    "claims_supported": [
        "sub",
        "aud",
        "iss",
        "exp"
    ],
    "subject_types_supported": [
        "public"
    ],
    "id_token_signing_alg_values_supported": [
        "RS256"
    ],
    "jwks_uri": "$1"
})";

void JWKSDiscoveryHandler(const Webserver::WebRequest& req,
                          Webserver::PrerenderedWebResponse* resp,
                          const JWKSMockServer& jwks_server) {
  auto* account_id = FindOrNull(req.parsed_args, "accountid");
  if (!account_id) {
    resp->output << "expected 'accountId' query";
    resp->status_code = HttpStatusCode::BadRequest;
    return;
  }
  resp->output << Substitute(kDiscoveryFormat, *account_id,
                             jwks_server.url_for_account(*account_id));
  resp->status_code = HttpStatusCode::Ok;
}

const string kValidAccount = "new-phone";
const string kInvalidAccount = "who-is-this";
const string kMissingAccount = "no-where";

class JWKSDiscoveryEndpointMockServer {
 public:
  Status Start() {
    unordered_map<string, string> account_id_to_resp({
        {
          // Create an account that has valid keys.
          kValidAccount,
          Substitute(jwks_rsa_file_format, kid_1, "RS256",
              rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
              rsa_pub_key_jwk_e),
        },
        {
          // The keys associated with this account are invalid.
          kInvalidAccount,
          Substitute(jwks_rsa_file_format, kid_1, "RS256",
              rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256",
              rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e),
        },
    });
    RETURN_NOT_OK(jwks_server_.StartWithAccounts(account_id_to_resp));

    WebserverOptions opts;
    opts.port = 0;
    webserver_.reset(new Webserver(std::move(opts)));
    webserver_->RegisterPrerenderedPathHandler(
        "/.well-known/openid-configuration'", "openid-configuration",
        // Pass the 'accountId' query arguments to return a response that
        // points to the JWKS endpoint for the account.
        [this] (const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
          JWKSDiscoveryHandler(req, resp, jwks_server_);
        },
        /*is_styled*/false, /*is_on_nav_bar*/false);
    RETURN_NOT_OK(webserver_->Start());
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(webserver_->GetBoundAddresses(&addrs));
    RETURN_NOT_OK(addr_.ParseString("127.0.0.1", addrs[0].port()));
    url_ = Substitute("http://$0/.well-known/openid-configuration'", addr_.ToString());
    return Status::OK();
  }

  const string& url() const {
    return url_;
  }
 private:
  unique_ptr<Webserver> webserver_;
  JWKSMockServer jwks_server_;
  string url_;
  Sockaddr addr_;
};

} // anonymous namespace

TEST(JwtUtilTest, VerifyJWKSDiscoveryEndpoint) {
  JWKSDiscoveryEndpointMockServer discovery_endpoint;
  ASSERT_OK(discovery_endpoint.Start());
  PerAccountKeyBasedJwtVerifier jwt_verifier(discovery_endpoint.url());
  {
    auto valid_user_token =
        jwt::create()
            .set_issuer(Substitute("auth0/$0", kValidAccount))
            .set_type("JWT")
            .set_algorithm("RS256")
            .set_key_id(kid_1)
            .set_subject(kValidAccount)
            .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
    string subject;
    ASSERT_OK(jwt_verifier.VerifyToken(valid_user_token, &subject));
    ASSERT_EQ(kValidAccount, subject);
  }
  {
    auto invalid_user_token =
        jwt::create()
            .set_issuer(Substitute("auth0/$0", kInvalidAccount))
            .set_type("JWT")
            .set_algorithm("RS256")
            .set_key_id(kid_1)
            .set_subject(kInvalidAccount)
            .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
    string subject;
    Status s = jwt_verifier.VerifyToken(invalid_user_token, &subject);
    ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  }
  {
    auto missing_user_token =
        jwt::create()
            .set_issuer(Substitute("auth0/$0", kMissingAccount))
            .set_type("JWT")
            .set_algorithm("RS256")
            .set_key_id(kid_1)
            .set_subject(kMissingAccount)
            .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
    string subject;
    Status s = jwt_verifier.VerifyToken(missing_user_token, &subject);
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  }
}

} // namespace kudu
