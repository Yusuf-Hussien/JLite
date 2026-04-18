package jlite.lock;
import jlite.transaction.Transaction;
/**
 * TODO: acquire(tx, resource, mode) with wait-for graph.
 * TODO: releaseAll(tx) on commit/rollback.
 * TODO: deadlock detection via cycle detection.
 * TODO: lock timeout + LockTimeoutException.
 */
public class LockManager {
    public void acquire(Transaction tx, String resourceId, LockMode mode) {
        throw new UnsupportedOperationException("LockManager.acquire() not yet implemented");
    }
    public void releaseAll(Transaction tx) {
        throw new UnsupportedOperationException("LockManager.releaseAll() not yet implemented");
    }
}
