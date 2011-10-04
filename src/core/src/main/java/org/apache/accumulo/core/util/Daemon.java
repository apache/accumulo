package org.apache.accumulo.core.util;

public class Daemon extends Thread {

    public Daemon() {
       setDaemon(true);
    }

    public Daemon(Runnable target) {
        super(target);
        setDaemon(true);
    }

    public Daemon(String name) {
        super(name);
        setDaemon(true);
    }

    public Daemon(ThreadGroup group, Runnable target) {
        super(group, target);
        setDaemon(true);
    }

    public Daemon(ThreadGroup group, String name) {
        super(group, name);
        setDaemon(true);
    }

    public Daemon(Runnable target, String name) {
        super(target, name);
        setDaemon(true);
    }

    public Daemon(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
        setDaemon(true);
    }

    public Daemon(ThreadGroup group, Runnable target, String name,
            long stackSize) {
        super(group, target, name, stackSize);
        setDaemon(true);
    }

}
