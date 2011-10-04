
from xml.sax import make_parser, handler

def parse(filename):
    'Read a accumulo config file and return it as a dictionary string -> string'
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

