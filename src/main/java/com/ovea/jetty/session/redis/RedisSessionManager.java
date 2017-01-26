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
package com.ovea.jetty.session.redis;

import static java.lang.Long.parseLong;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.InitialContext;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import com.ovea.jetty.session.Serializer;
import com.ovea.jetty.session.SerializerException;
import com.ovea.jetty.session.SessionManagerSkeleton;
import com.ovea.jetty.session.serializer.XStreamSerializer;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public final class RedisSessionManager extends SessionManagerSkeleton<RedisSessionManager.RedisSession> {

    private final static Logger LOG = Log.getLogger("com.ovea.jetty.session");

    private static final String FIELD_ID            = "id";
    private static final String FIELD_LAST_NODE     = "lastNode";
    private static final String FIELD_ACCESSED      = "accessed";
    private static final String FIELD_LAST_ACCESSED = "lastAccessed";
    private static final String FIELD_CREATED       = "created";
    private static final String FIELD_COOKIE_SET    = "cookieSet";
    private static final String FIELD_LAST_SAVED    = "lastSaved";
    private static final String FIELD_EXPIRY_TIME   = "expiryTime";
    private static final String FIELD_ATTRIBUTES    = "attributes";

    private static final String[] FIELDS_TO_LOAD = {FIELD_ID, FIELD_LAST_NODE, FIELD_ACCESSED, FIELD_LAST_ACCESSED,
            FIELD_CREATED, FIELD_COOKIE_SET, FIELD_LAST_SAVED, FIELD_EXPIRY_TIME, FIELD_ATTRIBUTES};

    private static final List<String> FIELDS_TO_LOAD_LIST = Arrays.asList(FIELDS_TO_LOAD);

    private static final int LOAD_IDX_ID            = FIELDS_TO_LOAD_LIST.indexOf(FIELD_ID);
    private static final int LOAD_IDX_LAST_NODE     = FIELDS_TO_LOAD_LIST.indexOf(FIELD_LAST_NODE);
    private static final int LOAD_IDX_ACCESSED      = FIELDS_TO_LOAD_LIST.indexOf(FIELD_ACCESSED);
    private static final int LOAD_IDX_LAST_ACCESSED = FIELDS_TO_LOAD_LIST.indexOf(FIELD_LAST_ACCESSED);
    private static final int LOAD_IDX_CREATED       = FIELDS_TO_LOAD_LIST.indexOf(FIELD_CREATED);
    private static final int LOAD_IDX_COOKIE_SET    = FIELDS_TO_LOAD_LIST.indexOf(FIELD_COOKIE_SET);
    private static final int LOAD_IDX_LAST_SAVED    = FIELDS_TO_LOAD_LIST.indexOf(FIELD_LAST_SAVED);
    private static final int LOAD_IDX_EXPIRY_TIME   = FIELDS_TO_LOAD_LIST.indexOf(FIELD_EXPIRY_TIME);
    private static final int LOAD_IDX_ATTRIBUTES    = FIELDS_TO_LOAD_LIST.indexOf(FIELD_ATTRIBUTES);

    private final JedisExecutor jedisExecutor;
    private final Serializer serializer;

    private final JedisExecutor jedisExecutorForLoadSession;
    private final Semaphore jedisExecutorForLoadSessionAvailable = new Semaphore(1, true);
    private final AtomicBoolean jedisExecutorForLoadSessionSet = new AtomicBoolean(false);


    private long staleIntervalSec = 10; // assume session to be fresh for 10 secs without refreshing from redis
    private long saveIntervalSec = 20; //only persist changes to session access times every 20 secs
    private boolean forceSaveAttributes = false; // when metadata updated, also updates session attributes

    public RedisSessionManager(JedisPool jedisPool) {
        this(jedisPool, new XStreamSerializer());
    }

    public RedisSessionManager(String jndiName) {
        this(jndiName, new XStreamSerializer());
    }

    public RedisSessionManager(JedisPool jedisPool, Serializer serializer) {
        this.serializer = serializer;
        this.jedisExecutor = new PooledJedisExecutor(jedisPool);
        this.jedisExecutorForLoadSession = getJedisExecutorForLoadSession();
    }

    public RedisSessionManager(final String jndiName, Serializer serializer) {
        this.serializer = serializer;
        this.jedisExecutor = getLazyJedisExecutor(jndiName);
        this.jedisExecutorForLoadSession = getJedisExecutorForLoadSession();
    }

    private JedisExecutor getLazyJedisExecutor(final String jndiName) {
        if (jndiName == null || jndiName.trim().isEmpty()) {
            throw new IllegalArgumentException();
        }
        return new JedisExecutor() {
            JedisExecutor delegate;

            public <V> V execute(JedisCallback<V> cb) {
                if (delegate == null) {
                    try {
                        InitialContext ic = new InitialContext();
                        JedisPool jedisPool = (JedisPool) ic.lookup(jndiName);
                        delegate = new PooledJedisExecutor(jedisPool);
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                "Unable to find instance of " + JedisExecutor.class.getName() + " in JNDI location "
                                        + jndiName + " : " + e.getMessage(), e);
                    }
                }
                return delegate.execute(cb);
            }
        };
    }

    private JedisExecutor getJedisExecutorForLoadSession() {
        if (jedisExecutor == null) {
            throw new IllegalStateException();
        }
        return new JedisExecutor() {
            public <V> V execute(JedisCallback<V> cb) {
                jedisExecutorForLoadSessionAvailable.acquireUninterruptibly();
                try {
                    return jedisExecutor.execute(cb);
                } finally {
                    jedisExecutorForLoadSessionAvailable.release();
                }
            }
        };
    }

    public void setStaleInterval(long sec) {
        staleIntervalSec = sec;
    }
    
    public void setSaveInterval(long sec) {
        saveIntervalSec = sec;
    }

    public void setForceSaveAttributes(boolean forceSaveAttributes) {
        this.forceSaveAttributes = forceSaveAttributes;
    }

    public void setMaxConcurrentLoadSession(int maxConcurrentLoadSession) {
        if (jedisExecutorForLoadSessionSet.compareAndSet(false, true)) {
            jedisExecutorForLoadSessionAvailable.release(maxConcurrentLoadSession - 1);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void doStart() throws Exception {
        serializer.start();
        super.doStart();
    }

    @Override
    public void doStop() throws Exception {
        super.doStop();
        serializer.stop();
    }
    
    @Override
    protected void shutdownSessions() throws Exception {
    }

    @Override
    protected boolean sessionReloadNeeded(RedisSession session) {
        // session이 sync된지 staleIntervalSec 만큼 지나지 않았으면 다시 redis에서 읽어올 필요가 없다
        // (staleIntervalSec 동안에는 session이 다른 서버로 옮겨갔다 돌아오지 않았다고 가정함)
        long now = System.currentTimeMillis();
        return (now - session.lastSynced) > (staleIntervalSec * 1000L);
    }

    @Override
    protected RedisSession loadSession(final String clusterId) {
        LOG.debug("[RedisSessionManager] loadSession - loading session from Redis id={}", clusterId);
        List<String> redisData;
        try {
            redisData = jedisExecutorForLoadSession.execute(new JedisCallback<List<String>>() {
                @Override
                public List<String> execute(Jedis jedis) {
                    final String key = RedisSessionIdManager.REDIS_SESSION_KEY + clusterId;
                    return jedis.hmget(key, FIELDS_TO_LOAD);
                }
            });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionManager] loadSession - redis exception", je);
            return null;
        }
        if (redisData.get(LOAD_IDX_ID) == null) {
            LOG.debug("[RedisSessionManager] loadSession - no session found in Redis for id={}", clusterId);
            return null;
        }
        String attrs = redisData.get(LOAD_IDX_ATTRIBUTES);
        //noinspection unchecked
        RedisSession loaded;
        try {
            loaded = new RedisSession(redisData, attrs == null ? new HashMap<String, Object>() :
                    serializer.deserialize(attrs, Map.class));
        } catch (SerializerException se) {
            LOG.warn("[RedisSessionManager] loadSession - Unable to deserialize because of class version mismatch");
            sessionIdManager.removeSession(clusterId);
            return null;
        }
        long now = System.currentTimeMillis();
        //if the session in the database has not already expired
        if (loaded.expiryTime <= 0 || loaded.expiryTime * 1000 > now) {
            LOG.debug("[RedisSessionManager] loadSession - loaded fresh session from Redis, id={}", loaded.getId());
            loaded.changeLastNode(getSessionIdManager().getWorkerName());
            return loaded;
        } else {
            LOG.debug("[RedisSessionManager] loadSession - loaded session has expired, id={}", clusterId);
            sessionIdManager.removeSession(clusterId);
            return null;
        }
    }

    @Override
    protected RedisSession reloadSession(final String clusterId, final RedisSession current) {
        LOG.debug("[RedisSessionManager] reloadSession - reloading session from Redis id={}", clusterId);
        // XXX: redisData가 null이면 변함이 없어서 갱신할 필요가 없었다는 것을 의미하며
        // emptyList이면 저장된 세션이 없다는 것을 의미
        List<String> redisData;
        try {
            redisData = jedisExecutorForLoadSession.execute(new JedisCallback<List<String>>() {
                @Override
                public List<String> execute(Jedis jedis) {
                    final String key = RedisSessionIdManager.REDIS_SESSION_KEY + clusterId;
                    String val = jedis.hget(key, "lastSaved");
                    if (val == null) {
                        // no session in store
                        return Collections.emptyList();
                    }
                    if (current.lastSaved != Long.parseLong(val)) {
                        // session has changed - reload
                        return jedis.hmget(key, FIELDS_TO_LOAD);
                    } else {
                        // session dit not changed in cache since last save
                        return null;
                    }
                }
            });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionManager] reloadSession - redis exception", je);
            current.lastSynced = System.currentTimeMillis();
            return current;
        }
        if (redisData == null) {
            // 세션이 그동안 갱신되지 않은 경우 - current를 그대로 사용 가능하다.
            // redis와 메모리에 있는 세션을 일치시킨 시점을 기록한 후 sessionReloadNeeded()에서 판단하는데 사용
            LOG.debug("[RedisSessionManager] reloadSession - no change found in Redis for session id={}", current.getId());
            current.lastSynced = System.currentTimeMillis();
            return current;
        }
        if (redisData.isEmpty() || redisData.get(LOAD_IDX_ID) == null) {
            LOG.debug("[RedisSessionManager] reloadSession - no session found in Redis for id={}", clusterId);
            return null;
        }
        String attrs = redisData.get(LOAD_IDX_ATTRIBUTES);
        //noinspection unchecked
        RedisSession reloaded = new RedisSession(redisData, attrs == null ? new HashMap<String, Object>() :
                serializer.deserialize(attrs, Map.class));
        long now = System.currentTimeMillis();
        if (reloaded.expiryTime <= 0 || reloaded.expiryTime * 1000 > now) {
            //if the session in the database has not already expired
            LOG.debug("[RedisSessionManager] loadSession - reloaded fresh session from Redis, id={}", reloaded.getId());
            if (!reloaded.lastNode.equals(getSessionIdManager().getWorkerName())) {
                //session last used on a different node
                reloaded.changeLastNode(getSessionIdManager().getWorkerName());
            }
            return reloaded;
        } else {
            LOG.debug("[RedisSessionManager] reloadSession - reloaded session has expired, id={}", clusterId);
            return null;
        }
    }

    @Override
    protected void storeSession(final RedisSession session) {
        session.lastSaved = session.lastSynced = System.currentTimeMillis();
        final Map<String, String> redisMap = new HashMap<String, String>();
        redisMap.put(FIELD_ID, session.getClusterId());
        redisMap.put(FIELD_LAST_NODE, session.lastNode);
        redisMap.put(FIELD_ACCESSED, Long.toString(session.getAccessed()));
        redisMap.put(FIELD_LAST_ACCESSED, Long.toString(session.getLastAccessedTime()));
        redisMap.put(FIELD_CREATED, Long.toString(session.getCreationTime()));
        redisMap.put(FIELD_COOKIE_SET, Long.toString(session.getCookieSetTime()));
        redisMap.put(FIELD_LAST_SAVED, Long.toString(session.lastSaved));
        redisMap.put(FIELD_EXPIRY_TIME, Long.toString(session.expiryTime));
        redisMap.put(FIELD_ATTRIBUTES, serializer.serialize(session.getSessionAttributes()));

        LOG.debug("[RedisSessionManager] storeSession - Storing session id={}", session.getClusterId());
        saveSessionMap(session, redisMap);
    }

    /**
     * 주어진 세선의 정보를 redis에 저장한다. session 파라미터에서 key 및 expire ttl 값을 얻으며,
     * map으로 주어진 데이터를 해당 key, ttl에 대해서 저장한다.
     *
     * @param session key 및 ttl 값을 얻어올 RedisSession
     * @param map 위 key, ttl 값에 대해 저장한 데이터 키/값
     */
    private void saveSessionMap(final RedisSession session, final Map<String, String> map) {
        final String key = RedisSessionIdManager.REDIS_SESSION_KEY + session.getClusterId();
        final int ttl = session.getMaxInactiveInterval();
        try {
            jedisExecutor.execute(new JedisCallback<Void>() {
                @Override
                public Void execute(Jedis jedis) {
                    Transaction t = jedis.multi();
                    t.hmset(key, map);
                    if (ttl > 0) {
                        t.expire(key, ttl);
                    }
                    t.exec();
                    return null;
                }
            });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionManager] saveSessionMap - redis exception", je);
        }
    }

    @Override
    protected void updateSession(final RedisSession session) {
        session.lastSaved = session.lastSynced = System.currentTimeMillis();
        Map<String, String> redisMap = populateUpdateSessionMap(session, true);
        LOG.debug("[RedisSessionManager] updateSession - Updating session id={}", session.getClusterId());
        saveSessionMap(session, redisMap);
    }

    private Map<String, String> populateUpdateSessionMap(final RedisSession session, boolean includeAttributes) {
        final Map<String, String> redisMap = new HashMap<String, String>();
        redisMap.put(FIELD_LAST_NODE, session.lastNode);
        redisMap.put(FIELD_ACCESSED, Long.toString(session.getAccessed()));
        redisMap.put(FIELD_LAST_ACCESSED, Long.toString(session.getLastAccessedTime()));
        redisMap.put(FIELD_EXPIRY_TIME, Long.toString(session.expiryTime));
        redisMap.put(FIELD_LAST_SAVED, Long.toString(session.lastSaved));
        if (includeAttributes) {
            redisMap.put(FIELD_ATTRIBUTES, serializer.serialize(session.getSessionAttributes()));
        }
        return redisMap;
    }

    @Override
    protected void updateSessionMetadata(final RedisSession session) {
        session.lastSaved = session.lastSynced = System.currentTimeMillis();
        Map<String, String> redisMap = populateUpdateSessionMap(session, false);
        LOG.debug("[RedisSessionManager] updateSessionMetadata - Update session metadata id={}", session.getClusterId());
        saveSessionMap(session, redisMap);
    }

    @Override
    protected RedisSession newSession(HttpServletRequest request) {
        return new RedisSession(request);
    }

    @Override
    protected void deleteSession(final RedisSession session) {
        LOG.debug("[RedisSessionManager] deleteSession - Deleting from Redis session id={}", session.getClusterId());
        try {
            jedisExecutor.execute(new JedisCallback<Object>() {
                @Override
                public Object execute(Jedis jedis) {
                    return jedis.del(RedisSessionIdManager.REDIS_SESSION_KEY + session.getClusterId());
                }
            });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionManager] deleteSession - redis exception", je);
        }
    }

    final class RedisSession extends SessionManagerSkeleton<?>.SessionSkeleton {

        private boolean dirty = false;
        private boolean forceUpdateMetadata = false;

        private long expiryTime;    /* 세션이 만료되는 시각. 초(s) 단위 */
        private long lastSaved;     /* 세션이 마지막으로 redis에 저장된 시각. 밀리초(ms) 단위 */
        private long lastSynced;    /* 세션이 redis에 저장된 것과 일치하는지 확인한 시각. 밀리초(ms) 단위 */
        private String lastNode;

        private RedisSession(HttpServletRequest request) {
            super(request);
            lastNode = getSessionIdManager().getWorkerName();
            long ttl = getMaxInactiveInterval();
            expiryTime = ttl <= 0 ? 0 : System.currentTimeMillis() / 1000 + ttl;
        }

        RedisSession(List<String> redisData, Map<String, Object> attributes) {
            super(parseLong(redisData.get(LOAD_IDX_CREATED)), parseLong(redisData.get(LOAD_IDX_ACCESSED)),
                    redisData.get(LOAD_IDX_ID));
            lastNode = redisData.get(LOAD_IDX_LAST_NODE);
            setLastAccessedTime(parseLong(redisData.get(LOAD_IDX_LAST_ACCESSED)));
            setCookieSetTime(parseLong(redisData.get(LOAD_IDX_COOKIE_SET)));
            lastSaved = parseLong(redisData.get(LOAD_IDX_LAST_SAVED));
            expiryTime = parseLong(redisData.get(LOAD_IDX_EXPIRY_TIME));
            lastSynced = System.currentTimeMillis();
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                super.doPutOrRemove(entry.getKey(), entry.getValue());
            }
        }

        void changeLastNode(String lastNode) {
            synchronized (this) {
                this.lastNode = lastNode;
                this.forceUpdateMetadata = true;
            }
        }

        @Override
        public void setAttribute(String name, Object value) {
            synchronized (this) {
                super.setAttribute(name, value);
                dirty = true;
            }
        }

        @Override
        public void removeAttribute(String name) {
            synchronized (this) {
                super.removeAttribute(name);
                dirty = true;
            }
        }

        // XXX: 아래 메소드는 불필요하고 AbstractSession.getAttributeMap()을 쓰면 될 것 같지만
        // serialize된 결과가 달라지고 따라서 구버전과 호환성이 없을 것 같으므로 그냥 둠
        final Map<String, Object> getSessionAttributes() {
            Map<String, Object> attrs = new LinkedHashMap<String, Object>();
            for (String key : super.getNames()) {
                attrs.put(key, super.doGet(key));
            }
            return attrs;
        }

        @Override
        protected boolean access(long time) {
            synchronized (this)
            {
                if (super.access(time)) {
                    int ttl = getMaxInactiveInterval();
                    expiryTime = ttl <= 0 ? 0 : time / 1000 + ttl;  // ttl, expiryTime은 초단위 임에 주의
                    return true;
                }
                return false;
            }
        }

        @Override
        protected void complete() {
            synchronized (this) {
                super.complete();
                try {
                    if (isValid()) {
                        if (dirty) {
                            willPassivate();
                            updateSession(this);
                            didActivate();
                        } else {
                            boolean timeToSaveMetadata = (getAccessed() - lastSaved) >= (saveIntervalSec * 1000);
                            if (timeToSaveMetadata || forceUpdateMetadata) {
                                if (forceSaveAttributes) {
                                    // metadata를 갱신할때 session attributes도 다시 저장한다
                                    updateSession(this);
                                } else {
                                    // metadata만 갱신
                                    updateSessionMetadata(this);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("[RedisSessionManager] complete - Problem persisting changed session data id=" + getId(), e);
                } finally {
                    dirty = false;
                    forceUpdateMetadata = false;
                }
            }
        }
    }
}
