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


import os

HERE = os.path.dirname(__file__)
ACCUMULO_HOME = os.getenv('ACCUMULO_HOME')
if not os.getenv('ACCUMULO_CONF_DIR'):
  ACCUMULO_CONF_DIR = ACCUMULO_HOME+'/conf'
else:
  ACCUMULO_CONF_DIR = os.getenv('ACCUMULO_CONF_DIR')

def accumulo(*args):
    return os.path.join(ACCUMULO_HOME, *args)

def accumuloConf(*args):
    return os.path.join(ACCUMULO_CONF_DIR, *args)

def accumuloJar():
    import glob
    options = (glob.glob(accumulo('lib', 'accumulo*.jar')) +
               glob.glob(accumulo('accumulo', 'target', 'accumulo*.jar')))
    options = [jar for jar in options if jar.find('instrumented') < 0]
    return options[0]

