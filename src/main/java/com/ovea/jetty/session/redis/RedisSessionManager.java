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

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.InitialContext;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import com.ovea.jetty.session.Serializer;
import com.ovea.jetty.session.SessionManagerSkeleton;
import com.ovea.jetty.session.serializer.XStreamSerializer;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public final class RedisSessionManager extends SessionManagerSkeleton<RedisSessionManager.RedisSession> {

    final static Logger LOG = Log.getLogger("com.ovea.jetty.session");
    private static final String[] FIELDS = {"id", "created", "accessed", "lastNode", "expiryTime", "lastSaved", "lastAccessed", "maxIdle", "cookieSet", "attributes"};

    private final JedisExecutor jedisExecutor;
    private final Serializer serializer;


    private long staleIntervalSec = 10; // assume session to be fresh for 10 secs without refreshing from redis
    
    private long saveIntervalSec = 20; //only persist changes to session access times every 20 secs

    public RedisSessionManager(JedisPool jedisPool) {
        this(jedisPool, new XStreamSerializer());
    }

    public RedisSessionManager(String jndiName) {
        this(jndiName, new XStreamSerializer());
    }

    public RedisSessionManager(JedisPool jedisPool, Serializer serializer) {
        this.serializer = serializer;
        this.jedisExecutor = new PooledJedisExecutor(jedisPool);
    }

    public RedisSessionManager(final String jndiName, Serializer serializer) {
        this.serializer = serializer;
        this.jedisExecutor = new JedisExecutor() {
            JedisExecutor delegate;

            @Override
            public <V> V execute(JedisCallback<V> cb) {
                if (delegate == null) {
                    try {
                        InitialContext ic = new InitialContext();
                        JedisPool jedisPool = (JedisPool) ic.lookup(jndiName);
                        delegate = new PooledJedisExecutor(jedisPool);
                    } catch (Exception e) {
                        throw new IllegalStateException("Unable to find instance of " + JedisExecutor.class.getName() + " in JNDI location " + jndiName + " : " + e.getMessage(), e);
                    }
                }
                return delegate.execute(cb);
            }
        };
    }

    public void setStaleInterval(long sec) {
        staleIntervalSec = sec;
    }
    
    public void setSaveInterval(long sec) {
        saveIntervalSec = sec;
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
    protected boolean sessionReloadNeeded(RedisSession session) {
        // session이 sync된지 staleIntervalSec 만큼 지나지 않았으면 다시 redis에서 읽어올 필요가 없다
        // (staleIntervalSec 동안에는 session이 다른 서버로 옮겨갔다 돌아오지 않았다고 가정할 수 있으므로)
        long now = System.currentTimeMillis();
        return (now - session.lastSynced) > (staleIntervalSec * 1000L);
    }

    @Override
    protected RedisSession loadSession(final String clusterId) {
        LOG.debug("[RedisSessionManager] loadSession - loading session from Redis id={}", clusterId);
        RedisSession loaded = loadFromStore(clusterId);
        if (loaded == null) {
            LOG.debug("[RedisSessionManager] loadSession - no session found in Redis for id={}", clusterId);
            return null;
        }
        long now = System.currentTimeMillis();
        //if the session in the database has not already expired
        if (loaded.expiryTime <= 0 || loaded.expiryTime * 1000 > now) {
            //session last used on a different node, or we don't have it in memory
            LOG.debug("[RedisSessionManager] loadSession - loaded fresh session from Redis, id={}", loaded.getId());
            if (!loaded.lastNode.equals(getSessionIdManager().getWorkerName())) {
                //session last used on a different node, or we don't have it in memory
                loaded.changeLastNode(getSessionIdManager().getWorkerName());
            }
            return loaded;
        } else {
            LOG.debug("[RedisSessionManager] loadSession - loaded session has expired, id={}", clusterId);
            return null;
        }
    }

    @Override
    protected RedisSession reloadSession(final String clusterId, final RedisSession current) {
        if (!current.requestStarted()) {
            // 한 스레드에서 세션이 두번이상 로드되는 것을 방지 (왜?)
            return current;
        }

        LOG.debug("[RedisSessionManager] reloadSession - reloading session from Redis id={}", clusterId);
        RedisSession reloaded = reloadFromStore(clusterId, current);
        if (reloaded == null) {
            LOG.debug("[RedisSessionManager] reloadSession - no session found in Redis for id={}", clusterId);
            return null;
        } else if (reloaded == current) {
            LOG.debug("[RedisSessionManager] reloadSession - no change found in Redis for session id={}", reloaded.getId());
            return reloaded;
        } else {
            long now = System.currentTimeMillis();
            if (reloaded.expiryTime <= 0 || reloaded.expiryTime * 1000 > now) {
                //if the session in the database has not already expired
                LOG.debug("[RedisSessionManager] loadSession - reloaded fresh session from Redis, id={}", reloaded.getId());
                if (!reloaded.lastNode.equals(getSessionIdManager().getWorkerName())) {
                    //session last used on a different node, or we don't have it in memory
                    reloaded.changeLastNode(getSessionIdManager().getWorkerName());
                }
                return reloaded;
            } else {
                LOG.debug("[RedisSessionManager] reloadSession - reloaded session has expired, id={}", clusterId);
                return null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private RedisSession loadFromStore(final String clusterId) {
        List<String> redisData = jedisExecutor.execute(new JedisCallback<List<String>>() {
            @Override
            public List<String> execute(Jedis jedis) {
                final String key = RedisSessionIdManager.REDIS_SESSION_KEY + clusterId;
                return jedis.exists(key) ? jedis.hmget(key, FIELDS) : null;
            }
        });
        if (redisData == null || redisData.isEmpty() || redisData.get(0) == null) {
            // no session found in redis
            return null;
        }
        Map<String, String> data = new HashMap<String, String>();
        for (int i = 0; i < FIELDS.length; i++)
            data.put(FIELDS[i], redisData.get(i));
        String attrs = data.get("attributes");
        //noinspection unchecked
        return new RedisSession(data, attrs == null ? new HashMap<String, Object>() :
                serializer.deserialize(attrs, Map.class));
    }

    @SuppressWarnings("unchecked")
	private RedisSession reloadFromStore(final String clusterId, final RedisSession current) {
        // XXX: redisData가 null이면 변함이 없어서 갱신할 필요가 없었다는 것을 의미하며
        // emptyList이면 저장된 세션이 없다는 것을 의미
        List<String> redisData = jedisExecutor.execute(new JedisCallback<List<String>>() {
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
                    return jedis.hmget(key, FIELDS);
                } else {
                    // session dit not changed in cache since last save
                    return null;
                }
            }
        });
        if (redisData == null) {
            // 세션이 그동안 갱신되지 않은 경우 - current를 그대로 사용 가능하다.
            // redis와 메모리에 있는 세션을 일치시킨 시점을 기록한 후 sessionReloadNeeded()에서 판단하는데 사용
            current.lastSynced = System.currentTimeMillis();
            return current;
        }
        if (redisData.isEmpty() || redisData.get(0) == null) {
            // redis에 저장된 세션이 없는 경우
            return null;
        }
        Map<String, String> data = new HashMap<String, String>();
        for (int i = 0; i < FIELDS.length; i++)
            data.put(FIELDS[i], redisData.get(i));
        String attrs = data.get("attributes");
        //noinspection unchecked
        return new RedisSession(data, attrs == null ? new HashMap<String, Object>() :
                serializer.deserialize(attrs, Map.class));
    }

    @Override
    protected void storeSession(final RedisSession session) {
        if (!session.redisMap.isEmpty()) {
            if (session.redisMap.containsKey("attributes"))
            	session.redisMap.put("attributes", serializer.serialize(session.getSessionAttributes()));
            LOG.debug("[RedisSessionManager] storeSession - Storing session id={}", session.getClusterId());
            jedisExecutor.execute(new JedisCallback<Void>() {
                @Override
                public Void execute(Jedis jedis) {
                    session.lastSaved = System.currentTimeMillis();
                    session.lastSynced = System.currentTimeMillis();
                    session.redisMap.put("lastSaved", Long.toString(session.lastSaved));
                    Transaction t = jedis.multi();
                    final String key = RedisSessionIdManager.REDIS_SESSION_KEY + session.getClusterId();
                    t.hmset(key, session.redisMap);
                    int ttl = session.getMaxInactiveInterval();
                    if (ttl > 0) {
                        t.expire(key, ttl);
                    }
                    t.exec();
                    return null;
                }
            });
            session.redisMap.clear();
        }
    }

    @Override
    protected RedisSession newSession(HttpServletRequest request) {
        return new RedisSession(request);
    }

    @Override
    protected void deleteSession(final RedisSession session) {
        LOG.debug("[RedisSessionManager] deleteSession - Deleting from Redis session id={}", session.getClusterId());
        jedisExecutor.execute(new JedisCallback<Object>() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.del(RedisSessionIdManager.REDIS_SESSION_KEY + session.getClusterId());
            }
        });
    }

    final class RedisSession extends SessionManagerSkeleton<?>.SessionSkeleton {

        private final Map<String, String> redisMap = new TreeMap<String, String>();

        private long expiryTime;
        private long lastSaved;
        private long lastSynced;
        private String lastNode;
        private final ThreadLocal<Boolean> firstAccess = new ThreadLocal<Boolean>() {
            @Override
            protected Boolean initialValue() {
                return true;
            }
        };

        private RedisSession(HttpServletRequest request) {
            super(request);
            lastNode = getSessionIdManager().getWorkerName();
            long ttl = getMaxInactiveInterval();
            expiryTime = ttl <= 0 ? 0 : System.currentTimeMillis() / 1000 + ttl;
            lastSynced = System.currentTimeMillis();
            // new session so prepare redis map accordingly
            redisMap.put("id", getClusterId());
            redisMap.put("context", getCanonicalizedContext());
            redisMap.put("virtualHost", getVirtualHost());
            redisMap.put("created", Long.toString(getCreationTime()));
            redisMap.put("lastNode", lastNode);
            redisMap.put("lastAccessed", Long.toString(getLastAccessedTime()));
            redisMap.put("accessed", Long.toString(getAccessed()));
            redisMap.put("expiryTime", Long.toString(expiryTime));
            redisMap.put("maxIdle", Long.toString(ttl));
            redisMap.put("cookieSet", Long.toString(getCookieSetTime()));
            redisMap.put("attributes", "");
        }

        RedisSession(Map<String, String> redisData, Map<String, Object> attributes) {
            super(parseLong(redisData.get("created")), parseLong(redisData.get("accessed")), redisData.get("id"));
            lastNode = redisData.get("lastNode");
            expiryTime = parseLong(redisData.get("expiryTime"));
            lastSaved = parseLong(redisData.get("lastSaved"));
            lastSynced = System.currentTimeMillis();
            super.setMaxInactiveInterval(parseInt(redisData.get("maxIdle")));
            setCookieSetTime(parseLong(redisData.get("cookieSet")));
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                super.doPutOrRemove(entry.getKey(), entry.getValue());
            }
            super.access(parseLong(redisData.get("lastAccessed")));
        }

        public void changeLastNode(String lastNode) {
            synchronized (this) {
                this.lastNode = lastNode;
                redisMap.put("lastNode", lastNode);
			}
        }

        @Override
        public void setAttribute(String name, Object value) {
            synchronized (this) {
	            super.setAttribute(name, value);
	            redisMap.put("attributes", "");
            }
        }

        @Override
        public void removeAttribute(String name) {
            synchronized (this) {
	            super.removeAttribute(name);
	            redisMap.put("attributes", "");
            }
        }

        public final Map<String, Object> getSessionAttributes() {
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
		            firstAccess.remove();
		            int ttl = getMaxInactiveInterval();
		            expiryTime = ttl <= 0 ? 0 : time / 1000 + ttl;
		            // prepare serialization
		            redisMap.put("lastAccessed", Long.toString(getLastAccessedTime()));
		            redisMap.put("accessed", Long.toString(getAccessed()));
		            redisMap.put("expiryTime", Long.toString(expiryTime));
		            return true;
	            }
	            return false;
            }
        }

        @Override
        public void setMaxInactiveInterval(int secs) {
            synchronized (this) {
	            super.setMaxInactiveInterval(secs);
	            // prepare serialization
	            redisMap.put("maxIdle", Long.toString(secs));
            }
        }

        @Override
        protected void cookieSet() {
            synchronized (this) {
	            super.cookieSet();
	            // prepare serialization
	            redisMap.put("cookieSet", Long.toString(getCookieSetTime()));
            }
        }

        @Override
        protected void complete() {
        	synchronized (this) {
	            super.complete();
                if (isValid()) {
		            if (!redisMap.isEmpty()
		                && (redisMap.size() != 3
		                || !redisMap.containsKey("lastAccessed")
		                || !redisMap.containsKey("accessed")
		                || !redisMap.containsKey("expiryTime")
		                || getAccessed() - lastSaved >= saveIntervalSec * 1000)) {
		                try {
		                    willPassivate();
		                    storeSession(this);	
		                    didActivate();
		                } catch (Exception e) {
		                    LOG.warn("[RedisSessionManager] complete - Problem persisting changed session data id=" + getId(), e);
		                } finally {
		                    redisMap.clear();
		                }
		            }
                }
            }
        }

        public boolean requestStarted() {
            boolean first = firstAccess.get();
            if (first)
                firstAccess.set(false);
            return first;
        }
    }
}
