package jlite.wal;
/**
 * TODO: append to log file with fsync on commit.
 * TODO: ARIES redo pass on startup.
 * TODO: checkpoint mechanism.
 */
public class WalManager {
    public WalManager(String logPath) {}
    public long append(WalRecord record) {
        throw new UnsupportedOperationException("WalManager.append() not yet implemented");
    }
    public void flush() {
        throw new UnsupportedOperationException("WalManager.flush() not yet implemented");
    }
}
