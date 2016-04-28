/**
 * Copyright (C) 2011 Ovea <dev@ovea.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ovea.jetty.session;

import static java.lang.Math.round;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.eclipse.jetty.server.session.AbstractSession;
import org.eclipse.jetty.server.session.AbstractSessionManager;
import org.eclipse.jetty.util.LazyList;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public abstract class SessionManagerSkeleton<T extends SessionManagerSkeleton<?>.SessionSkeleton> extends AbstractSessionManager {

    private final static Logger LOG = Log.getLogger("com.ovea.jetty.session");
    private static final Field _cookieSet;

    static {
        try {
            _cookieSet = AbstractSession.class.getDeclaredField("_cookieSet");
            _cookieSet.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private final ConcurrentMap<String, T> sessions = new ConcurrentHashMap<String, T>();
    

    @Override
    public void doStart() throws Exception {
        sessions.clear();
        super.doStart();
    }

    @Override
    public void doStop() throws Exception {
        sessions.clear();
        super.doStop();
    }

    @Override
    protected final void addSession(final AbstractSession session) {
        if (isRunning()) {
            @SuppressWarnings({"unchecked"}) T sessionSkeleton = (T) session;
            String clusterId = getClusterId(session);
            LOG.debug("[SessionManagerSkeleton] add new session id={}", session.getId());

            sessions.put(clusterId, sessionSkeleton);
            try
            {
                sessionSkeleton.willPassivate();
                storeSession(sessionSkeleton);
                sessionSkeleton.didActivate();
            }
            catch (Exception e)
            {
                LOG.warn("[SessionManagerSkeleton] Unable to store new session id=" + session.getId() , e);
            }
        }
    }

    @Override
    public final void removeSession(AbstractSession sess, boolean invalidate) {
        @SuppressWarnings({"unchecked"}) T session = (T) sess;
        String clusterId = getClusterId(session);
        boolean removed = removeSession(clusterId);
        if (removed) {
            _sessionsStats.decrement();
            _sessionTimeStats.set(round((System.currentTimeMillis() - session.getCreationTime()) / 1000.0));
            _sessionIdManager.removeSession(session);
            if (invalidate)
                _sessionIdManager.invalidateAll(session.getClusterId());
            if (invalidate && _sessionListeners != null) {
                HttpSessionEvent event = new HttpSessionEvent(session);
                for (int i = LazyList.size(_sessionListeners); i-- > 0; )
                    ((HttpSessionListener) LazyList.get(_sessionListeners, i)).sessionDestroyed(event);
            }
            if (!invalidate) {
                session.willPassivate();
            }
        }
    }

    @Override
    protected final boolean removeSession(String clusterId) {
        LOG.debug("[SessionManagerSkeleton] removing session id={}", clusterId);
        T session = sessions.remove(clusterId);
        try {
            if (session != null) {
                LOG.debug("[SessionManagerSkeleton] removed session id={}", session.getId());
                deleteSession(session);
            }
        } catch (Exception e) {
            LOG.warn("[SessionManagerSkeleton] Problem deleting session id=" + clusterId, e);
        }
        return session != null;
    }

    @Override
    public final T getSession(String clusterId) {
        final T current = sessions.get(clusterId);
        if (current == null) {
            // 세션이 메모리에 있는 없는 경우
            T loaded = loadSession(clusterId);
            if (loaded == null) {
                // 세션이 메모리에도 없고 redis에도 없는 경우
                LOG.debug("[SessionManagerSkeleton] failed to load session for cluster id={}", clusterId);
                return null;
            } else {
                // 세션이 메모리에 없어 redis에서 읽어온 경우
                LOG.debug("[SessionManagerSkeleton] loaded session id={}", loaded.getId());
                T prev = sessions.putIfAbsent(clusterId, loaded);
                if (prev == null) {
                    loaded.didActivate();
                    return loaded;
                } else {
                    return prev;
                }
            }
        } else {
            // current != null 인 경우, 즉 세션이 메모리에 있는 경우
            synchronized (current) {
                // 다른 스레드에서 put 했을 수도 있으므로 다시 get 해 온다
                T current2 = sessions.get(clusterId);
                if (current2 != current) {
                    // 다른 스레드에서 뭔가 바꿨으면 처음부터 다시 시작
                    return getSession(clusterId);
                }
                if (sessionReloadNeeded(current)) {
                    // 메모리에 있는 세션의 유효성을 다시 확인하기로 한 경우 (lastSynced 시간 기준)
                    T reloaded = reloadSession(clusterId, current);
                    if (reloaded == null) {
                        // redis에는 세션이 없는 경우... 메모리에 있는게 유효하지 않는 것으로 판단
                        LOG.debug("[SessionManagerSkeleton] failed to reload session for cluster id={}", clusterId);
                        return null;
                    } else if (reloaded == current) {
                        // redis에 세션이 있는데 변경점이 없어서 메모리에 있는 것을 그대로 사용 가능 (lastSaved 기준)
                        LOG.debug("[SessionManagerSkeleton] session synced w/o reload id={}", reloaded.getId());
                        return reloaded;
                    } else {
                        // redis에 세션이 있는데 메모리와 있는 것과 일치하지 않아서 다시 읽어온 경우
                        // (세션이 한개 이상의 서버에서 접근 되었음을 의미)
                        LOG.debug("[SessionManagerSkeleton] reloaded session id={}", reloaded.getId());
                        sessions.put(clusterId, reloaded);
                        reloaded.didActivate();
                        return reloaded;
                    }
                } else {
                    // 메모리에 있는 세션의 유효성을 다시 확인하지도 않으면서 그대로 사용 할 수 있는 경우
                    return current;
                }
            }
        }
    }
    
    protected abstract boolean sessionReloadNeeded(T session);

    @Override
    @Deprecated
    public final Map<String, T> getSessionMap() {
        return Collections.unmodifiableMap(sessions);
    }

    @Override
    protected final void invalidateSessions() {
        //Do nothing - we don't want to remove and
        //invalidate all the sessions because this
        //method is called from doStop(), and just
        //because this context is stopping does not
        //mean that we should remove the session from
        //any other nodes
    }

    public final void invalidateSession(String clusterId) {
        AbstractSession session = sessions.get(clusterId);
        if (session != null)
            session.invalidate();
    }

    public final void expire(List<String> expired) {
        if (isStopping() || isStopped())
            return;
        ClassLoader old_loader = Thread.currentThread().getContextClassLoader();
        try {
            for (String expiredClusterId : expired) {
                LOG.debug("[SessionManagerSkeleton] Expiring session id={}", expiredClusterId);
                T session = sessions.get(expiredClusterId);
                if (session != null)
                    session.timeout();
            }
        } catch (Throwable t) {
            if (t instanceof ThreadDeath) {
                throw ((ThreadDeath) t);
            } else {
                LOG.warn("[SessionManagerSkeleton] Problem expiring sessions", t);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(old_loader);
        }
    }
    
    public final void setSessionPath(String path) {
        getSessionCookieConfig().setPath(path);
    }

    public final void setMaxCookieAge(int seconds) {
        getSessionCookieConfig().setMaxAge(seconds);
    }

    public void setSessionDomain(String domain) {
        setDomain(domain);
    }

    public void setDomain(String domain) {
        getSessionCookieConfig().setDomain(domain);
    }

    protected final String getVirtualHost() {
        String vhost = "0.0.0.0";
        if (_context == null)
            return vhost;
        String[] vhosts = _context.getContextHandler().getVirtualHosts();
        if (vhosts == null || vhosts.length == 0 || vhosts[0] == null)
            return vhost;
        return vhosts[0];
    }

    protected final String getCanonicalizedContext() {
        if (_context.getContextPath() == null) return "";
        return _context.getContextPath().replace('/', '_').replace('.', '_').replace('\\', '_');
    }

    protected abstract void storeSession(T session);

    protected abstract void deleteSession(T session);

    protected abstract T loadSession(String clusterId);

    protected abstract T reloadSession(String clusterId, T current);

    public abstract class SessionSkeleton extends AbstractSession {

        public SessionSkeleton(HttpServletRequest request) {
            super(SessionManagerSkeleton.this, request);
            super.setMaxInactiveInterval(SessionManagerSkeleton.this._dftMaxIdleSecs > 0 ? SessionManagerSkeleton.this._dftMaxIdleSecs : -1);
        }

        public SessionSkeleton(long created, long accessed, String clusterId) {
            super(SessionManagerSkeleton.this, created, accessed, clusterId);
        }

        @Override
        public void timeout() throws IllegalStateException {
            LOG.debug("[SessionManagerSkeleton] Timing out session id={}", getClusterId());
            super.timeout();
        }

        protected void setCookieSetTime(long time) {
            try {
                _cookieSet.set(this, time);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
