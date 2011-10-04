import os

HERE = os.path.dirname(__file__)
ACCUMULO_HOME = os.path.normpath(
    os.path.join(HERE, *(os.pardir,)*4)
    )

def accumulo(*args):
    return os.path.join(ACCUMULO_HOME, *args)

def accumuloJar():
    import glob
    options = (glob.glob(accumulo('lib', 'accumulo*.jar')) +
               glob.glob(accumulo('accumulo', 'target', 'accumulo*.jar')))
    options = [jar for jar in options if jar.find('instrumented') < 0]
    return options[0]

