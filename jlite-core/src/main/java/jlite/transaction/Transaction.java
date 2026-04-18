package jlite.transaction;
import java.util.UUID;
/**
 * TODO: track read/write sets for MVCC.
 * TODO: undo log for rollback.
 */
public class Transaction {
    public enum State { ACTIVE, COMMITTED, ABORTED }
    private final String id;
    private final IsolationLevel isolationLevel;
    private State state;
    public Transaction(IsolationLevel isolationLevel) {
        this.id = UUID.randomUUID().toString();
        this.isolationLevel = isolationLevel;
        this.state = State.ACTIVE;
    }
    public String getId() { return id; }
    public IsolationLevel getIsolationLevel() { return isolationLevel; }
    public State getState() { return state; }
    public void setState(State state) { this.state = state; }
}
