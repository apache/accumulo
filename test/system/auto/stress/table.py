import unittest
import os
from TestUtils import TestUtilsMixin, ROOT, ROOT_PASSWORD, ACCUMULO_DIR

class Table(TestUtilsMixin, unittest.TestCase):
    "create and destroy the same table repeatedly, sometimes ingesting into it"

    order = 85

    def deleteTable(self, table):
        out, err, code = self.rootShell(self.masterHost(),
                                        "deletetable %s\n" % table)
        self.processResult(out, err, code)
        self.sleep(0.5)
        
    def tables(self):
        code, out, err = self.shell(self.masterHost(), "tables\n")
        self.processResult(out, err, code)
        return out

    def runTest(self):
        import tempfile
        fileno, self.filename = tempfile.mkstemp()
        fp = os.fdopen(fileno, "wb")
        try:
            for i in range(0, 200, 10):
                fp.write("%08x\n" % (i * 1000))
            fp.close()

            for i in range(5):
                self.createTable('test_ingest')
                self.deleteTable('test_ingest')
            self.createTable('test_ingest')
            for i in range(5):
                self.wait(self.ingest(self.masterHost(), 10, start=i*10))
                self.wait(self.verify(self.masterHost(), 10, start=i*10))
                self.deleteTable('test_ingest')
                self.createTable('test_ingest', self.filename)
        finally:
            os.unlink(self.filename)
        
def suite():
    result = unittest.TestSuite()
    result.addTest(Table())
    return result
