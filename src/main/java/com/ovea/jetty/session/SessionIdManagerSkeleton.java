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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.session.AbstractSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public abstract class SessionIdManagerSkeleton extends AbstractSessionIdManager {

    private final static Logger LOG = Log.getLogger("com.ovea.jetty.session");
    // for a session id in the whole jetty, each webapp can have different sessions for the same id
    private final Set<String> sessions = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final Server server;

    private long scavengerInterval = 60 * 1000; // 1min
    private ScheduledFuture<?> scavenger;
    private ScheduledExecutorService executorService;

    protected SessionIdManagerSkeleton(Server server) {
        this.server = server;
    }

    public final void setScavengerInterval(long scavengerInterval) {
        this.scavengerInterval = scavengerInterval;
    }

    @Override
    protected final void doStart() throws Exception {
        sessions.clear();
        if (scavenger != null) {
            scavenger.cancel(true);
            scavenger = null;
        }
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (scavengerInterval > 0) {
            executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("RedisSessionIdManager-ScavengerThread");
                    return t;
                }
            });
            scavenger = executorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    if (!sessions.isEmpty()) {
                        try {
                            final List<String> expired = scavenge(new ArrayList<String>(sessions));
                            for (String clusterId : expired)
                                sessions.remove(clusterId);
                            forEachSessionManager(new SessionManagerCallback() {
                                @Override
                                public void execute(SessionManagerSkeleton<?> sessionManager) {
                                    sessionManager.expire(expired);
                                }
                            });
                        } catch (Exception e) {
                            LOG.warn("Scavenger thread failure: " + e.getMessage(), e);
                        }
                    }
                }
            }, scavengerInterval, scavengerInterval, TimeUnit.MILLISECONDS);
        }
        super.doStart();
    }

    @Override
    protected final void doStop() throws Exception {
        sessions.clear();
        if (scavenger != null) {
            scavenger.cancel(true);
            scavenger = null;
        }
        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        super.doStop();
    }

    @Override
    public final String getClusterId(String nodeId) {
        int dot = nodeId.lastIndexOf('.');
        return dot > 0 ? nodeId.substring(0, dot) : nodeId;
    }

    @Override
    public final String getNodeId(String clusterId, HttpServletRequest request) {
        if (_workerName != null)
            return clusterId + '.' + _workerName;
        return clusterId;
    }

    @Override
    public final boolean idInUse(String id) {
        String cid = getClusterId(id);
        return id != null && (sessions.contains(cid) || hasClusterId(cid));
    }

    @Override
    public final void addSession(HttpSession session) {
        String clusterId = getClusterId(session.getId());
        storeClusterId(clusterId);
        sessions.add(clusterId);
    }

    @Override
    public final void removeSession(HttpSession session) {
        if (session == null) {
            return;
        }
        String clusterId = getClusterId(session.getId());
        removeSession(clusterId);
    }

    public void removeSession (String clusterId) {
        if (clusterId == null)
            return;

        LOG.debug("Removing session clusterId={}", clusterId);
        try {
            sessions.remove(clusterId);
            deleteClusterId(clusterId);
        } catch (Exception e) {
            LOG.warn("Problem removing session clusterId=" + clusterId, e);
        }
    }

    @Override
    public final void invalidateAll(final String clusterId) {
        removeSession(clusterId);
        forEachSessionManager(new SessionManagerCallback() {
            @Override
            public void execute(SessionManagerSkeleton<?> sessionManager) {
                sessionManager.invalidateSession(clusterId);
            }
        });
    }

    protected abstract void deleteClusterId(String clusterId);

    protected abstract void storeClusterId(String clusterId);

    protected abstract boolean hasClusterId(String clusterId);

    protected abstract List<String> scavenge(List<String> clusterIds);

    private void forEachSessionManager(SessionManagerCallback callback) {
        Handler[] contexts = server.getChildHandlersByClass(ContextHandler.class);
        for (int i = 0; contexts != null && i < contexts.length; i++) {
            SessionHandler sessionHandler = ((ContextHandler) contexts[i]).getChildHandlerByClass(SessionHandler.class);
            if (sessionHandler != null) {
                SessionManager manager = sessionHandler.getSessionManager();
                if (manager != null && manager instanceof SessionManagerSkeleton)
                    callback.execute((SessionManagerSkeleton<?>) manager);
            }
        }
    }

    /**
     * @author Mathieu Carbou (mathieu.carbou@gmail.com)
     */
    private interface SessionManagerCallback {
        void execute(SessionManagerSkeleton<?> sessionManager);
    }
}
