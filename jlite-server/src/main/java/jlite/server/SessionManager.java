package jlite.server;

import jlite.transaction.IsolationLevel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-connection session state: auth, isolation level, current transaction.
 *
 * TODO: session authentication (username/password or token).
 * TODO: configurable default isolation level.
 * TODO: session variable store (SET key=value per session).
 */
public class SessionManager {

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    public Session createSession(String sessionId) {
        var session = new Session(sessionId, IsolationLevel.READ_COMMITTED);
        sessions.put(sessionId, session);
        return session;
    }

    public void closeSession(String sessionId) {
        sessions.remove(sessionId);
    }

    public record Session(String id, IsolationLevel defaultIsolation) {}
}
