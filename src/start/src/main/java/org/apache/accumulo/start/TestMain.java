package org.apache.accumulo.start;


public class TestMain {
	public static void main(String[] args) {
	    if (args.length > 0) {
	        if (args[0].equals("success"))
	            System.exit(0);
	        if (args[0].equals("throw"))
	            throw new RuntimeException("This is an exception");
	    }
        System.exit(-1);
    }
}
