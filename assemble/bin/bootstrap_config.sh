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

function usage {
  cat <<EOF
Usage: bootstrap_config.sh [-options]
where options include (long options not available on all platforms):
    -d, --dir        Alternate directory to setup config files
    -s, --size       Supported sizes: '1GB' '2GB' '3GB' '512MB'
    -n, --native     Configure to use native libraries
    -j, --jvm        Configure to use the jvm
    -o, --overwrite  Overwrite the default config directory
    -v, --version    Specify the Apache Hadoop version supported versions: '1' '2'
    -k, --kerberos   Configure for use with Kerberos
    -h, --help       Print this help message
EOF
}

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# Stop: Resolve Script Directory

#
# Resolve accumulo home for bootstrapping
#
ACCUMULO_HOME=$( cd -P ${bin}/.. && pwd )
TEMPLATE_CONF_DIR="${ACCUMULO_HOME}/conf/templates"
CONF_DIR="${ACCUMULO_HOME}/conf"
ACCUMULO_SITE=accumulo-site.xml
ACCUMULO_ENV=accumulo-env.sh

SIZE=
TYPE=
HADOOP_VERSION=
OVERWRITE="0"
BASE_DIR=
KERBEROS=

#Execute getopt
if [[ $(uname -s) == "Linux" ]]; then
  args=$(getopt -o "b:d:s:njokv:h" -l "basedir:,dir:,size:,native,jvm,overwrite,kerberos,version:,help" -q -- "$@")
else # Darwin, BSD
  args=$(getopt b:d:s:njokv:h $*)
fi

#Bad arguments
if [[ $? != 0 ]]; then
  usage 1>&2
  exit 1
fi
eval set -- $args

for i
do
  case "$i" in
    -b|--basedir) #Hidden option used to set general.maven.project.basedir for developers
      BASE_DIR=$2; shift
      shift;;
    -d|--dir)
      CONF_DIR=$2; shift
      shift;;
    -s|--size)
      SIZE=$2; shift
      shift;;
    -n|--native)
      TYPE=native
      shift;;
    -j|--jvm)
      TYPE=jvm
      shift;;
    -o|--overwrite)
      OVERWRITE=1
      shift;;
    -v|--version)
      HADOOP_VERSION=$2; shift
      shift;;
    -k|--kerberos)
      KERBEROS="true"
      shift;;
    -h|--help)
      usage
      exit 0
      shift;;
    --)
    shift
    break;;
  esac
done

while [[ "${OVERWRITE}" = "0" ]]; do
  if [[ -e "${CONF_DIR}/${ACCUMULO_ENV}" || -e "${CONF_DIR}/${ACCUMULO_SITE}" ]]; then
    echo "Warning your current config files in ${CONF_DIR} will be overwritten!"
    echo
    echo "How would you like to proceed?:"
    select CHOICE in 'Continue with overwrite' 'Specify new conf dir'; do
      if [[ "${CHOICE}" = 'Specify new conf dir' ]]; then
        echo -n "Please specifiy new conf directory: "
        read CONF_DIR
      elif [[ "${CHOICE}" = 'Continue with overwrite' ]]; then
        OVERWRITE=1
      fi
      break
    done
  else
    OVERWRITE=1
  fi
done
echo "Copying configuration files to: ${CONF_DIR}"

#Native 1GB
native_1GB_tServer="-Xmx128m -Xms128m"
_1GB_master="-Xmx128m -Xms128m"
_1GB_monitor="-Xmx64m -Xms64m"
_1GB_gc="-Xmx64m -Xms64m"
_1GB_other="-Xmx128m -Xms64m"
_1GB_shell="${_1GB_other}"

_1GB_memoryMapMax="256M"
native_1GB_nativeEnabled="true"
_1GB_cacheDataSize="15M"
_1GB_cacheIndexSize="40M"
_1GB_sortBufferSize="50M"
_1GB_waLogMaxSize="256M"

#Native 2GB
native_2GB_tServer="-Xmx256m -Xms256m"
_2GB_master="-Xmx256m -Xms256m"
_2GB_monitor="-Xmx128m -Xms64m"
_2GB_gc="-Xmx128m -Xms128m"
_2GB_other="-Xmx256m -Xms64m"
_2GB_shell="${_2GB_other}"

_2GB_memoryMapMax="512M"
native_2GB_nativeEnabled="true"
_2GB_cacheDataSize="30M"
_2GB_cacheIndexSize="80M"
_2GB_sortBufferSize="50M"
_2GB_waLogMaxSize="512M"

#Native 3GB
native_3GB_tServer="-Xmx1g -Xms1g -XX:NewSize=500m -XX:MaxNewSize=500m"
_3GB_master="-Xmx1g -Xms1g"
_3GB_monitor="-Xmx1g -Xms256m"
_3GB_gc="-Xmx256m -Xms256m"
_3GB_other="-Xmx1g -Xms256m"
_3GB_shell="${_3GB_other}"

_3GB_memoryMapMax="1G"
native_3GB_nativeEnabled="true"
_3GB_cacheDataSize="128M"
_3GB_cacheIndexSize="128M"
_3GB_sortBufferSize="200M"
_3GB_waLogMaxSize="1G"

#Native 512MB
native_512MB_tServer="-Xmx48m -Xms48m"
_512MB_master="-Xmx128m -Xms128m"
_512MB_monitor="-Xmx64m -Xms64m"
_512MB_gc="-Xmx64m -Xms64m"
_512MB_other="-Xmx128m -Xms64m"
_512MB_shell="${_512MB_other}"

_512MB_memoryMapMax="80M"
native_512MB_nativeEnabled="true"
_512MB_cacheDataSize="7M"
_512MB_cacheIndexSize="20M"
_512MB_sortBufferSize="50M"
_512MB_waLogMaxSize="100M"

#JVM 1GB
jvm_1GB_tServer="-Xmx384m -Xms384m"

jvm_1GB_nativeEnabled="false"

#JVM 2GB
jvm_2GB_tServer="-Xmx768m -Xms768m"

jvm_2GB_nativeEnabled="false"

#JVM 3GB
jvm_3GB_tServer="-Xmx2g -Xms2g -XX:NewSize=1G -XX:MaxNewSize=1G"

jvm_3GB_nativeEnabled="false"

#JVM 512MB
jvm_512MB_tServer="-Xmx128m -Xms128m"

jvm_512MB_nativeEnabled="false"


if [[ -z "${SIZE}" ]]; then
  echo "Choose the heap configuration:"
  select DIRNAME in 1GB 2GB 3GB 512MB; do
    echo "Using '${DIRNAME}' configuration"
    SIZE=${DIRNAME}
    break
  done
elif [[ "${SIZE}" != "1GB" && "${SIZE}" != "2GB"  && "${SIZE}" != "3GB" && "${SIZE}" != "512MB" ]]; then
  echo "Invalid memory size"
  echo "Supported sizes: '1GB' '2GB' '3GB' '512MB'"
  exit 1
fi

if [[ -z "${TYPE}" ]]; then
  echo
  echo "Choose the Accumulo memory-map type:"
  select TYPENAME in Java Native; do
    if [[ "${TYPENAME}" == "Native" ]]; then
      TYPE="native"
      echo "Don't forget to build the native libraries using the bin/build_native_library.sh script"
    elif [[ "${TYPENAME}" == "Java" ]]; then
      TYPE="jvm"
    fi
    echo "Using '${TYPE}' configuration"
    echo
    break
  done
fi

if [[ -z "${HADOOP_VERSION}" ]]; then
  echo
  echo "Choose the Apache Hadoop version:"
  select HADOOP in 'Hadoop 2' 'HDP 2.0/2.1' 'HDP 2.2' ; do
    if [ "${HADOOP}" == "Hadoop 2" ]; then
      HADOOP_VERSION="2"
    elif [ "${HADOOP}" == "HDP 2.0/2.1" ]; then
      HADOOP_VERSION="HDP2"
    elif [ "${HADOOP}" == "HDP 2.2" ]; then
      HADOOP_VERSION="HDP2.2"
    fi
    echo "Using Hadoop version '${HADOOP_VERSION}' configuration"
    echo
    break
  done
elif [[ "${HADOOP_VERSION}" != "2" && "${HADOOP_VERSION}" != "HDP2" && "${HADOOP_VERSION}" != "HDP2.2" ]]; then
  echo "Invalid Hadoop version"
  echo "Supported Hadoop versions: '2', 'HDP2', 'HDP2.2'"
  exit 1
fi

TRACE_USER="root"

if [[ ! -z "${KERBEROS}" ]]; then
  echo
  read -p "Enter server's Kerberos principal: " PRINCIPAL
  read -p "Enter server's Kerberos keytab: " KEYTAB
  TRACE_USER="${PRINCIPAL}"
fi

for var in SIZE TYPE HADOOP_VERSION; do
  if [[ -z ${!var} ]]; then
    echo "Invalid $var configuration"
    exit 1
  fi
done

TSERVER="${TYPE}_${SIZE}_tServer"
MASTER="_${SIZE}_master"
MONITOR="_${SIZE}_monitor"
GC="_${SIZE}_gc"
SHELL="_${SIZE}_shell"
OTHER="_${SIZE}_other"

MEMORY_MAP_MAX="_${SIZE}_memoryMapMax"
NATIVE="${TYPE}_${SIZE}_nativeEnabled"
CACHE_DATA_SIZE="_${SIZE}_cacheDataSize"
CACHE_INDEX_SIZE="_${SIZE}_cacheIndexSize"
SORT_BUFFER_SIZE="_${SIZE}_sortBufferSize"
WAL_MAX_SIZE="_${SIZE}_waLogMaxSize"

MAVEN_PROJ_BASEDIR=""

if [[ ! -z "${BASE_DIR}" ]]; then
  MAVEN_PROJ_BASEDIR="\n  <property>\n    <name>general.maven.project.basedir</name>\n    <value>${BASE_DIR}</value>\n  </property>\n"
fi

#Configure accumulo-env.sh
mkdir -p "${CONF_DIR}" && cp ${TEMPLATE_CONF_DIR}/* ${CONF_DIR}/
sed -e "s/\${tServerHigh_tServerLow}/${!TSERVER}/" \
    -e "s/\${masterHigh_masterLow}/${!MASTER}/" \
    -e "s/\${monitorHigh_monitorLow}/${!MONITOR}/" \
    -e "s/\${gcHigh_gcLow}/${!GC}/" \
    -e "s/\${shellHigh_shellLow}/${!SHELL}/" \
    -e "s/\${otherHigh_otherLow}/${!OTHER}/" \
    ${TEMPLATE_CONF_DIR}/$ACCUMULO_ENV > ${CONF_DIR}/$ACCUMULO_ENV

#Configure accumulo-site.xml
sed -e "s/\${memMapMax}/${!MEMORY_MAP_MAX}/" \
    -e "s/\${nativeEnabled}/${!NATIVE}/" \
    -e "s/\${cacheDataSize}/${!CACHE_DATA_SIZE}/" \
    -e "s/\${cacheIndexSize}/${!CACHE_INDEX_SIZE}/" \
    -e "s/\${sortBufferSize}/${!SORT_BUFFER_SIZE}/" \
    -e "s/\${waLogMaxSize}/${!WAL_MAX_SIZE}/" \
    -e "s=\${traceUser}=${TRACE_USER}=" \
    -e "s=\${mvnProjBaseDir}=${MAVEN_PROJ_BASEDIR}=" ${TEMPLATE_CONF_DIR}/$ACCUMULO_SITE > ${CONF_DIR}/$ACCUMULO_SITE

# If we're not using kerberos, filter out the krb properties
if [[ -z "${KERBEROS}" ]]; then
  sed -e 's/<!-- Kerberos requirements -->/<!-- Kerberos requirements --><!--/' \
      -e 's/<!-- End Kerberos requirements -->/--><!-- End Kerberos requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
else
  # Make the substitutions
  sed -e "s!\${keytab}!${KEYTAB}!" \
      -e "s!\${principal}!${PRINCIPAL}!" \
      ${CONF_DIR}/${ACCUMULO_SITE} > temp
  mv temp ${CONF_DIR}/${ACCUMULO_SITE}
fi

# Configure hadoop version
if [[ "${HADOOP_VERSION}" == "2" ]]; then
  sed -e 's/<!-- HDP 2.0 requirements -->/<!-- HDP 2.0 requirements --><!--/' \
      -e 's/<!-- End HDP 2.0 requirements -->/--><!-- End HDP 2.0 requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
  sed -e 's/<!-- HDP 2.2 requirements -->/<!-- HDP 2.2 requirements --><!--/' \
      -e 's/<!-- End HDP 2.2 requirements -->/--><!-- End HDP 2.2 requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
elif [[ "${HADOOP_VERSION}" == "HDP2" ]]; then
  sed -e 's/<!-- Hadoop 2 requirements -->/<!-- Hadoop 2 requirements --><!--/' \
      -e 's/<!-- End Hadoop 2 requirements -->/--><!-- End Hadoop 2 requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
  sed -e 's/<!-- HDP 2.2 requirements -->/<!-- HDP 2.2 requirements --><!--/' \
      -e 's/<!-- End HDP 2.2 requirements -->/--><!-- End HDP 2.2 requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
elif [[ "${HADOOP_VERSION}" == "HDP2.2" ]]; then
  sed -e 's/<!-- Hadoop 2 requirements -->/<!-- Hadoop 2 requirements --><!--/' \
      -e 's/<!-- End Hadoop 2 requirements -->/--><!-- End Hadoop 2 requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
  sed -e 's/<!-- HDP 2.0 requirements -->/<!-- HDP 2.0 requirements --><!--/' \
      -e 's/<!-- End HDP 2.0 requirements -->/--><!-- End HDP 2.0 requirements -->/' \
      "${CONF_DIR}/$ACCUMULO_SITE" > temp
  mv temp "${CONF_DIR}/$ACCUMULO_SITE"
fi

#Additional setup steps for native configuration.
if [[ ${TYPE} == native ]]; then
  if [[ $(uname) == Linux ]]; then
    if [[ -z $HADOOP_PREFIX ]]; then
      echo "HADOOP_PREFIX not set cannot automatically configure LD_LIBRARY_PATH"
    else
      NATIVE_LIB=$(readlink -ef $(dirname $(for x in $(find $HADOOP_PREFIX -name libhadoop.so); do ld $x 2>/dev/null && echo $x && break; done) 2>>/dev/null) 2>>/dev/null)
      if [[ -z $NATIVE_LIB ]]; then
        echo -e "Native libraries could not be found for your sytem in: $HADOOP_PREFIX"
      else
        sed "/# Should the monitor/ i export LD_LIBRARY_PATH=${NATIVE_LIB}:\${LD_LIBRARY_PATH}" ${CONF_DIR}/$ACCUMULO_ENV > temp
        mv temp "${CONF_DIR}/$ACCUMULO_ENV"
        echo -e "Added ${NATIVE_LIB} to the LD_LIBRARY_PATH"
      fi
    fi
  fi
  echo -e "Please remember to compile the native libraries using the bin/build_native_library.sh script and to set the LD_LIBRARY_PATH variable in the ${CONF_DIR}/accumulo-env.sh script if needed."
fi
echo "Setup complete"
