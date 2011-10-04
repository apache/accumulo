package org.apache.accumulo.core.util;

import java.util.Date;

import org.apache.log4j.Logger;

public class LoggingRunnable implements Runnable {
    private Runnable runnable;
    private Logger log;

    public LoggingRunnable(Logger log, Runnable r){
        this.runnable = r;
        this.log = log;
    }
    
    public void run() {
        try{
            runnable.run();
        }catch(Throwable t){
            try{
                log.error("Thread \""+Thread.currentThread().getName()+"\" died "+t.getMessage(), t);
            }catch(Throwable t2){
                //maybe the logging system is screwed up OR there is a bug in the exception, like t.getMessage() throws a NPE
                System.err.println("ERROR "+new Date()+" Failed to log message about thread death "+t2.getMessage());
                t2.printStackTrace();
                
                //try to print original exception
                System.err.println("ERROR "+new Date()+" Exception that failed to log : "+t.getMessage());
                t.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) {
		Runnable r = new Runnable(){
			@Override
			public void run() {
				int x[] = new int[0];
				
				x[0]++;
			}
		};
		
		LoggingRunnable lr = new LoggingRunnable(null, r);
		lr.run();
		
	}
    
}
