package jlite.server;

import java.util.concurrent.Semaphore;

/**
 * Bounded connection pool managed via virtual threads.
 *
 * Waiting acquirers park their virtual thread cheaply rather than blocking an OS thread.
 *
 * TODO: idle connection timeout and cleanup.
 * TODO: acquisition latency histogram for monitoring.
 * TODO: configurable min-idle, max-total.
 */
public class ConnectionPool {

    private final int maxConnections;
    private final Semaphore semaphore;

    public ConnectionPool(int maxConnections) {
        this.maxConnections = maxConnections;
        this.semaphore = new Semaphore(maxConnections, true);
    }

    public void acquire() throws InterruptedException {
        semaphore.acquire(); // virtual thread parks here if pool is exhausted
    }

    public void release() {
        semaphore.release();
    }
}
