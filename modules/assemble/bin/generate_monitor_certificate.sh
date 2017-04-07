#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
   SOURCE=$(readlink "$SOURCE")
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
# Stop: Resolve Script Directory

. "$bin"/config.sh

ALIAS="default"
KEYPASS=$(LC_CTYPE=C tr -dc '#-~' < /dev/urandom | tr -d '<>&' | head -c 20)
STOREPASS=$(LC_CTYPE=C tr -dc '#-~' < /dev/urandom | tr -d '<>&' | head -c 20)
KEYSTOREPATH="$ACCUMULO_HOME/conf/keystore.jks"
TRUSTSTOREPATH="$ACCUMULO_HOME/conf/cacerts.jks"
CERTPATH="$ACCUMULO_HOME/conf/server.cer"

if [[ -e "$KEYSTOREPATH" ]]; then
   rm -i "$KEYSTOREPATH"
   if [[ -e "$KEYSTOREPATH" ]]; then
      echo "KeyStore already exists, exiting"
      exit 1
   fi
fi

if [[ -e "$TRUSTSTOREPATH" ]]; then
   rm -i "$TRUSTSTOREPATH"
   if [[ -e "$TRUSTSTOREPATH" ]]; then
      echo "TrustStore already exists, exiting"
      exit 2
   fi
fi

if [[ -e "$CERTPATH" ]]; then
   rm -i "$CERTPATH"
   if [[ -e "$CERTPATH" ]]; then
      echo "Certificate already exists, exiting"
      exit 3
  fi
fi

"${JAVA_HOME}/bin/keytool" -genkey -alias "$ALIAS" -keyalg RSA -keypass "$KEYPASS" -storepass "$KEYPASS" -keystore "$KEYSTOREPATH"
"${JAVA_HOME}/bin/keytool" -export -alias "$ALIAS" -storepass "$KEYPASS" -file "$CERTPATH" -keystore "$KEYSTOREPATH"
"${JAVA_HOME}/bin/keytool" -import -v -trustcacerts -alias "$ALIAS" -file "$CERTPATH" -keystore "$TRUSTSTOREPATH" -storepass "$STOREPASS" <<< "yes"

echo
echo "keystore and truststore generated.  now add the following to accumulo-site.xml:"
echo
echo "    <property>"
echo "      <name>monitor.ssl.keyStore</name>"
echo "      <value>$KEYSTOREPATH</value>"
echo "    </property>"
echo "    <property>"
echo "      <name>monitor.ssl.keyStorePassword</name>"
echo "      <value>$KEYPASS</value>"
echo "    </property>"
echo "    <property>"
echo "      <name>monitor.ssl.trustStore</name>"
echo "      <value>$TRUSTSTOREPATH</value>"
echo "    </property>"
echo "    <property>"
echo "      <name>monitor.ssl.trustStorePassword</name>"
echo "      <value>$STOREPASS</value>"
echo "    </property>"
echo
