package jlite.transaction;
import java.util.concurrent.ConcurrentHashMap;
/**
 * TODO: begin/commit/rollback with WAL coordination.
 * TODO: MVCC snapshot assignment.
 * TODO: vacuum / dead-tuple cleanup.
 */
public class TransactionManager {
    private final ConcurrentHashMap<String, Transaction> active = new ConcurrentHashMap<>();
    public Transaction begin(IsolationLevel level) {
        var tx = new Transaction(level);
        active.put(tx.getId(), tx);
        return tx;
    }
    public void commit(Transaction tx) {
        tx.setState(Transaction.State.COMMITTED);
        active.remove(tx.getId());
    }
    public void rollback(Transaction tx) {
        tx.setState(Transaction.State.ABORTED);
        active.remove(tx.getId());
    }
}
