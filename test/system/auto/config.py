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

from xml.sax import make_parser, handler

def parse(filename):
    'Read an accumulo config file and return it as a dictionary string -> string'
    result = {}
    class Handler(handler.ContentHandler):
        name = None
        content = ''
        
        def startElement(self, name, atts):
            self.content = ''
            
        def characters(self, data):
            self.content += data

        def endElement(self, name):
            if name == 'value' and self.name != None:
                result[self.name] = str(self.content).strip()
                self.name = None
            if name == 'name':
                self.name = str(self.content).strip()
            self.content = ''

    p = make_parser()
    p.setContentHandler(Handler())
    p.parse(filename)
    return result

