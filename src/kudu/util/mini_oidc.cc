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
#include "kudu/util/mini_oidc.h"

#include <string>
#include <vector>

#include <jwt-cpp/jwt.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/util/jwt_test_certs.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

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

void OidcDiscoveryHandler(const Webserver::WebRequest& req,
                          Webserver::PrerenderedWebResponse* resp,
                          const string& jwks_server_url) {
  auto* account_id = FindOrNull(req.parsed_args, "accountid");
  if (!account_id) {
    resp->output << "expected 'accountId' query";
    resp->status_code = HttpStatusCode::BadRequest;
    return;
  }
  resp->output << Substitute(kDiscoveryFormat, *account_id,
                             Substitute("$0/$1", jwks_server_url, *account_id));
  resp->status_code = HttpStatusCode::Ok;
}

} // anonymous namespace

MiniOidc::MiniOidc(MiniOidcOptions options)
    : options_(std::move(options)) {
  if (options_.expiration_time.empty()) {
    // XXX:
  }
}

// Explicitly defined outside the header to ensure users of the header don't
// need to include member destructors.
MiniOidc::~MiniOidc() {
  oidc_server_->Stop();
  jwks_server_->Stop();
}

Status MiniOidc::Start() {
  // Start the JWKS server and register path handlers for each of the accounts
  // we've been configured to server.
  {
    WebserverOptions opts;
    opts.port = 0;
    jwks_server_.reset(new Webserver(std::move(opts)));
  }
  for (const auto& [account_id, valid] : options_.account_ids) {
    jwks_server_->RegisterPrerenderedPathHandler(Substitute("/jwks/$0", account_id), account_id,
        [account_id = account_id, valid = valid] (const Webserver::WebRequest& req,
                                                  Webserver::PrerenderedWebResponse* resp) {
          // NOTE: 'kid_1' points at a valid key, while 'kid_2' points at an
          // invalid key.
          resp->output << Substitute(jwks_rsa_file_format, kid_1, "RS256",
              valid ? rsa_pub_key_jwk_n : rsa_invalid_pub_key_jwk_n,
              rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e),
          resp->status_code = HttpStatusCode::Ok;
        },
        /*is_styled*/false, /*is_on_nav_bar*/false);
  }
  RETURN_NOT_OK(jwks_server_->Start());
  vector<Sockaddr> bound_addrs;
  Sockaddr addr;
  RETURN_NOT_OK(jwks_server_->GetBoundAddresses(&bound_addrs));
  RETURN_NOT_OK(addr.ParseString("127.0.0.1", bound_addrs[0].port()));
  string jwks_url = Substitute("http://$0/jwks", addr.ToString());

  // Now start the OIDC Discovery server that points to the JWKS endpoints.
  WebserverOptions opts;
  opts.port = 0;
  oidc_server_.reset(new Webserver(std::move(opts)));
  oidc_server_->RegisterPrerenderedPathHandler(
      "/.well-known/openid-configuration", "openid-configuration",
      // Pass the 'accountId' query arguments to return a response that
      // points to the JWKS endpoint for the account.
      [jwks_url] (const Webserver::WebRequest& req, Webserver::PrerenderedWebResponse* resp) {
        OidcDiscoveryHandler(req, resp, jwks_url);
      },
      /*is_styled*/false, /*is_on_nav_bar*/false);
  RETURN_NOT_OK(oidc_server_->Start());
  bound_addrs.clear();
  RETURN_NOT_OK(oidc_server_->GetBoundAddresses(&bound_addrs));
  RETURN_NOT_OK(addr.ParseString("127.0.0.1", bound_addrs[0].port()));
  oidc_url_ = Substitute("http://$0/.well-known/openid-configuration", addr.ToString());
  return Status::OK();
}

void MiniOidc::Stop() {
  oidc_server_->Stop();
  jwks_server_->Stop();
}

string MiniOidc::CreateJwt(const string& account_id, const string& subject, bool is_valid) const {
 return jwt::create()
     .set_issuer(Substitute("auth0/$0", account_id))
     .set_type("JWT")
     .set_algorithm("RS256")
     .set_key_id(is_valid ? kid_1 : kid_2)
     .set_subject(subject)
     .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
}

} // namespace kudu
