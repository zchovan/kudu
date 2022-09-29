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

#include "kudu/util/status.h"
namespace kudu {

extern std::string rsa_priv_key_pem;
extern std::string rsa_pub_key_pem;
// The public keys in JWK format were converted from PEM formatted crypto keys with
// pem-to-jwk tool at https://hub.docker.com/r/danedmunds/pem-to-jwk/
extern std::string rsa_pub_key_jwk_n;
extern std::string rsa_pub_key_jwk_e;
extern std::string rsa_invalid_pub_key_jwk_n;

extern std::string rsa512_priv_key_pem;
extern std::string rsa512_pub_key_pem;
extern std::string rsa512_pub_key_jwk_n;
extern std::string rsa512_pub_key_jwk_e;
extern std::string rsa512_invalid_pub_key_jwk_n;

extern std::string rsa1024_priv_key_pem;
extern std::string rsa1024_pub_key_pem;
extern std::string rsa1024_pub_key_jwk_n;
extern std::string rsa1024_pub_key_jwk_e;

extern std::string rsa2048_priv_key_pem;
extern std::string rsa2048_pub_key_pem;
extern std::string rsa2048_pub_key_jwk_n;
extern std::string rsa2048_pub_key_jwk_e;

extern std::string rsa4096_priv_key_pem;
extern std::string rsa4096_pub_key_pem;
extern std::string rsa4096_pub_key_jwk_n;
extern std::string rsa4096_pub_key_jwk_e;

extern std::string ecdsa521_priv_key_pem;
extern std::string ecdsa521_pub_key_pem;
extern std::string ecdsa521_pub_key_jwk_x;
extern std::string ecdsa521_pub_key_jwk_y;

extern std::string ecdsa384_priv_key_pem;
extern std::string ecdsa384_pub_key_pem;
extern std::string ecdsa384_pub_key_jwk_x;
extern std::string ecdsa384_pub_key_jwk_y;

extern std::string ecdsa256_priv_key_pem;
extern std::string ecdsa256_pub_key_pem;
extern std::string ecdsa256_pub_key_jwk_x;
extern std::string ecdsa256_pub_key_jwk_y;

extern std::string kid_1;
extern std::string kid_2;

extern std::string jwks_hs_file_format;
extern std::string jwks_rsa_file_format;
extern std::string jwks_ec_file_format;

std::string CreateTestJWT(bool is_valid);
Status CreateTestJWKSFile(const std::string& dir, const std::string& file_name);

}  // namespace kudu
