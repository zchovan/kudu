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

#include "kudu/gutil/strings/substitute.h"

namespace kudu {
namespace ranger_kms {

// Gets the contents of the install.properties file used by the db_setup.py
// script.
inline std::string GetRangerKMSInstallProperties(const std::string& bin_dir,
                                                 const std::string& pg_host,
                                                 uint16_t pg_port,
                                                 const std::string& ranger_host,
                                                 uint16_t ranger_port) {
  // Taken and modified from:
  // https://github.com/apache/ranger/blob/master/kms/scripts/install.properties
  //
  // $0: directory containing postgresql.jar
  // $1: postgres host
  // $2: postgres port
  // $3: ranger admin host
  // $4: ranger admin port
  const char* kRangerKMSInstallPropertiesTemplate = R"(
PYTHON_COMMAND_INVOKER=python
DB_FLAVOR=POSTGRES
SQL_CONNECTOR_JAR=$0/postgresql.jar
db_root_user=postgres
db_root_password=
db_host=$1:$2
db_ssl_enabled=false
db_ssl_required=false
db_ssl_verifyServerCertificate=false
db_name=rangerkms
db_user=minirangerkms
db_password=
ranger_kms_http_enabled=true
KMS_MASTER_KEY_PASSWD=Str0ngPassw0rd
POLICY_MGR_URL=$3:$4
REPOSITORY_NAME=kmsdev
XAAUDIT.SUMMARY.ENABLE=false
XAAUDIT.ELASTICSEARCH.ENABLE=false
XAAUDIT.HDFS.ENABLE=false
XAAUDIT.LOG4J.ENABLE=false
mysql_core_file=db/mysql/kms_core_db.sql
oracle_core_file=db/oracle/kms_core_db_oracle.sql
postgres_core_file=db/postgres/kms_core_db_postgres.sql
sqlserver_core_file=db/sqlserver/kms_core_db_sqlserver.sql
sqlanywhere_core_file=db/sqlanywhere/kms_core_db_sqlanywhere.sql
KMS_BLACKLIST_DECRYPT_EEK=hdfs)";

  return strings::Substitute(kRangerKMSInstallPropertiesTemplate,
                             bin_dir, pg_host, pg_port, ranger_host, ranger_port);
}

inline std::string GetRangerKMSSiteXml(const std::string& kms_host,
                                       uint16_t kms_port) {
  const char* kRangerKMSSiteXmlTemplate = R"(
<configuration>
	<property>
		<name>ranger.service.host</name>
		<value>$0</value>
	</property>

	<property>
		<name>ranger.service.http.port</name>
		<value>$0</value>
	</property>

	<property>
		<name>ranger.service.shutdown.port</name>
		<value>$1</value>
	</property>

	<property>
		<name>ranger.contextName</name>
		<value>/kms</value>
	</property>

	<property>
		<name>xa.webapp.dir</name>
		<value>./webapp</value>
	</property>
	<property>
		<name>ranger.service.https.attrib.ssl.enabled</name>
		<value>false</value>
	</property>
	<property>
		<name>ajp.enabled</name>
		<value>false</value>
	</property>
	<property>
		<name>ranger.service.https.attrib.client.auth</name>
		<value>want</value>
	</property>
	<property>
		<name>ranger.credential.provider.path</name>
		<value>/etc/ranger/kms/rangerkms.jceks</value>
	</property>
	<property>
		<name>ranger.service.https.attrib.keystore.file</name>
		<value />
	</property>
	<property>
		<name>ranger.service.https.attrib.keystore.keyalias</name>
		<value>rangerkms</value>
	</property>
	<property>
		<name>ranger.service.https.attrib.keystore.pass</name>
		<value />
	</property>
	<property>
		<name>ranger.service.https.attrib.keystore.credential.alias</name>
		<value>keyStoreCredentialAlias</value>
	</property>

</configuration>)";
  return strings::Substitute(kRangerKMSSiteXmlTemplate, kms_host, kms_port);
}

inline std::string GetRangerKMSDbksSiteXml(const std::string pg_host, const uint16_t pg_port, const std::string& pg_driver) {
  const char *kRangerKMSDbksSiteXmlTemplate = R"(
<configuration>
  <property>
    <name>hadoop.kms.blacklist.DECRYPT_EEK</name>
    <value>hdfs</value>
    <description>
          Blacklist for decrypt EncryptedKey
          CryptoExtension operations
    </description>
  </property>
        <property>
                <name>ranger.db.encrypt.key.password</name>
                <value>_</value>
                <description>
                        Password used for encrypting Master Key
                </description>
        </property>
        <property>
                <name>ranger.kms.service.masterkey.password.cipher</name>
                <value>AES</value>
                <description>
                        Cipher used for encrypting Master Key
                </description>
        </property>
        <property>
               <name>ranger.kms.service.masterkey.password.size</name>
               <value>256</value>
                <description>
                        Size of masterkey
                </description>
       </property>
        <property>
                <name>ranger.kms.service.masterkey.password.salt.size</name>
                <value>8</value>
                <description>
                        Salt size to encrypt Master Key
                </description>
        </property>
        <property>
                <name>ranger.kms.service.masterkey.password.salt</name>
                <value>abcdefghijklmnopqrstuvwxyz01234567890</value>
                <description>
                        Salt to encrypt Master Key
                </description>
        </property>
        <property>
                <name>ranger.kms.service.masterkey.password.iteration.count</name>
                <value>1000</value>
                <description>
                        Iteration count to encrypt Master Key
                </description>
        </property>
        <property>
                <name>ranger.kms.service.masterkey.password.encryption.algorithm</name>
                <value>PBEWithMD5AndDES</value>
                <description>
                        Algorithm to encrypt Master Key
                </description>
        </property>
        <property>
                <name>ranger.kms.service.masterkey.password.md.algorithm</name>
                <value>SHA</value>
                <description>
                        Message Digest algorithn to encrypt Master Key
                </description>
        </property>
  <property>
    <name>ranger.ks.jpa.jdbc.url</name>
    <value>jdbc:postgresql://$0:$1/rangerkms</value>
    <description>
      URL for Database
    </description>
  </property>

  <property>
    <name>ranger.ks.jpa.jdbc.user</name>
    <value>rangerkms</value>
    <description>
      Database username used for operation
    </description>
  </property>

  <property>
    <name>ranger.ks.jpa.jdbc.password</name>
    <value></value>
    <description>
      Database user's password
    </description>
  </property>

  <property>
    <name>ranger.ks.jpa.jdbc.credential.provider.path</name>
    <value>/root/ranger-2.1.0-kms/ews/webapp/WEB-INF/classes/conf/.jceks/rangerkms.jceks</value>
    <description>
      Credential provider path
    </description>
  </property>

  <property>
    <name>ranger.ks.jpa.jdbc.credential.alias</name>
    <value>ranger.ks.jpa.jdbc.credential.alias</value>
    <description>
      Credential alias used for password
    </description>
  </property>

  <property>
    <name>ranger.ks.masterkey.credential.alias</name>
    <value>ranger.ks.masterkey.password</value>
    <description>
      Credential alias used for masterkey
    </description>
  </property>

  <property>
    <name>ranger.ks.jpa.jdbc.dialect</name>
    <value>org.eclipse.persistence.platform.database.PostgreSQLPlatform</value>
    <description>
      Dialect used for database
    </description>
  </property>

  <property>
    <name>ranger.ks.jpa.jdbc.driver</name>
    <value>org.postgresql.Driver</value>
    <description>
      Driver used for database
    </description>
  </property>

  <property>
    <name>ranger.ks.jdbc.sqlconnectorjar</name>
    <value>$2</value>
    <description>
      Driver used for database
    </description>
  </property>


  <property>
  	<name>ranger.ks.kerberos.principal</name>
  	<value>rangerkms/_HOST@REALM</value>
  </property>

  <property>
  	<name>ranger.ks.kerberos.keytab</name>
  	<value />
  </property>

  <property>
        <name>ranger.kms.keysecure.enabled</name>
        <value>false</value>
        <description />
  </property>

  <property>
        <name>ranger.kms.keysecure.UserPassword.Authentication</name>
        <value>true</value>
        <description />
  </property>
  <property>
        <name>ranger.kms.keysecure.masterkey.name</name>
        <value>safenetmasterkey</value>
        <description>Safenet key secure master key name</description>
  </property>
    <property>
        <name>ranger.kms.keysecure.login.username</name>
        <value>user1</value>
        <description>Safenet key secure username</description>
  </property>
  <property>
        <name>ranger.kms.keysecure.login.password</name>
        <value>t1e2s3t4</value>
        <description>Safenet key secure user password</description>
  </property>
  <property>
        <name>ranger.kms.keysecure.login.password.alias</name>
        <value>ranger.ks.login.password</value>
        <description>Safenet key secure user password</description>
  </property>
  <property>
        <name>ranger.kms.keysecure.hostname</name>
        <value>SunPKCS11-keysecurehn</value>
        <description>Safenet key secure hostname</description>
  </property>
  <property>
        <name>ranger.kms.keysecure.masterkey.size</name>
        <value>256</value>
        <description>key size</description>
  </property>
  <property>
        <name>ranger.kms.keysecure.sunpkcs11.cfg.filepath</name>
        <value>/opt/safenetConf/64/8.3.1/sunpkcs11.cfg</value>
        <description>Location of Safenet key secure library configuration file</description>
  </property>
  <property>
        <name>ranger.kms.keysecure.provider.type</name>
        <value>SunPKCS11</value>
        <description>Security Provider for key secure</description>
  </property>

  <property>
	<name>ranger.ks.db.ssl.enabled</name>
	<value>false</value>
  </property>
    <property>
	<name>ranger.ks.db.ssl.required</name>
	<value>false</value>
  </property>
    <property>
	<name>ranger.ks.db.ssl.verifyServerCertificate</name>
	<value>false</value>
  </property>
  <property>
	<name>ranger.ks.db.ssl.auth.type</name>
	<value>2-way</value>
  </property>
</configuration>
)";
  return strings::Substitute(kRangerKMSDbksSiteXmlTemplate, pg_host, pg_port, pg_driver);
}

inline std::string GetRangerKMSLog4jProperties(const std::string& log_level) {
  // log4j.properties file
  //
  // This is the default log4j.properties with the only difference that rootLogger
  // is made configurable if it's needed for debugging.
  //
  // $0: log level
  const char *kLog4jPropertiesTemplate = R"(
log4j.appender.kms=org.apache.log4j.DailyRollingFileAppender
log4j.appender.kms.DatePattern='.'yyyy-MM-dd
log4j.appender.kms.File=${kms.log.dir}/ranger-kms-${hostname}-${user}.log
log4j.appender.kms.Append=true
log4j.appender.kms.layout=org.apache.log4j.PatternLayout
log4j.appender.kms.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n

log4j.appender.kms-audit=org.apache.log4j.DailyRollingFileAppender
log4j.appender.kms-audit.DatePattern='.'yyyy-MM-dd
log4j.appender.kms-audit.File=${kms.log.dir}/kms-audit-${hostname}-${user}.log
log4j.appender.kms-audit.Append=true
log4j.appender.kms-audit.layout=org.apache.log4j.PatternLayout
log4j.appender.kms-audit.layout.ConversionPattern=%d{ISO8601} %m%n

log4j.logger.kms-audit=INFO, kms-audit
log4j.additivity.kms-audit=false

log4j.logger=$0, kms
log4j.rootLogger=WARN, kms
log4j.logger.org.apache.hadoop.conf=INFO
log4j.logger.org.apache.hadoop=INFO
log4j.logger.org.apache.ranger=INFO
log4j.logger.com.sun.jersey.server.wadl.generators.WadlGeneratorJAXBGrammarGenerator=OFF
)";
  return strings::Substitute(kLog4jPropertiesTemplate, log_level);
}

inline std::string GetRangerKMSSecurityXml(const std::string& ranger_host,
                                           uint16_t ranger_port) {
  const char *kRangerKmsSecurityXmlTemplate = R"(
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?><configuration xmlns:xi="http://www.w3.org/2001/XInclude">
	<property>
		<name>ranger.plugin.kms.service.name</name>
		<value>kmsdev</value>
		<description>
			Name of the Ranger service containing policies for this kms instance
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.source.impl</name>
		<value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
		<description>
			Class to retrieve policies from the source
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.rest.url</name>
		<value>https://$0:$1</value>
		<description>
			URL to Ranger Admin
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.rest.ssl.config.file</name>
		<value>/etc/kms/conf/ranger-policymgr-ssl.xml</value>
		<description>
			Path to the file containing SSL details to contact Ranger Admin
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.pollIntervalMs</name>
		<value>30000</value>
		<description>
			How often to poll for changes in policies?
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.cache.dir</name>
		<value>/etc/ranger/kmsdev/policycache</value>
		<description>
			Directory where Ranger policies are cached after successful retrieval from the source
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.rest.client.connection.timeoutMs</name>
		<value>120000</value>
		<description>
			RangerRestClient Connection Timeout in Milli Seconds
		</description>
	</property>

	<property>
		<name>ranger.plugin.kms.policy.rest.client.read.timeoutMs</name>
		<value>30000</value>
		<description>
			RangerRestClient read Timeout in Milli Seconds
		</description>
	</property>
</configuration>
)";
  return strings::Substitute(kRangerKmsSecurityXmlTemplate, ranger_host, ranger_port);
}

} // namespace ranger_kms
} // namespace kudu