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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.naming.InitialContext;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import com.ovea.jetty.session.SessionIdManagerSkeleton;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public final class RedisSessionIdManager extends SessionIdManagerSkeleton {

    private final static Logger LOG = Log.getLogger("com.ovea.jetty.session");

    private static final String REDIS_SESSIONS_KEY = "jetty-sessions";
    static final String REDIS_SESSION_KEY = "jetty-session-";

    private final JedisExecutor jedisExecutor;

    public RedisSessionIdManager(Server server, JedisPool jedisPool) {
        super(server);
        this.jedisExecutor = new PooledJedisExecutor(jedisPool);
    }

    public RedisSessionIdManager(Server server, final String jndiName) {
        super(server);
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

    @Override
    protected void deleteClusterId(final String clusterId) {
        try {
            jedisExecutor.execute(new JedisCallback<Object>() {
            @Override
            public Object execute(Jedis jedis) {
                return jedis.srem(REDIS_SESSIONS_KEY, clusterId);
            }
        });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionIdManager] deleteClusterId - redis exception", je);
        }
    }

    @Override
    protected void storeClusterId(final String clusterId) {
        try {
            jedisExecutor.execute(new JedisCallback<Object>() {
                @Override
                public Object execute(Jedis jedis) {
                    return jedis.sadd(REDIS_SESSIONS_KEY, clusterId);
                }
            });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionIdManager] storeClusterId - redis exception", je);
        }
    }

    @Override
    protected boolean hasClusterId(final String clusterId) {
        try {
            return jedisExecutor.execute(new JedisCallback<Boolean>() {
                @Override
                public Boolean execute(Jedis jedis) {
                    return jedis.sismember(REDIS_SESSIONS_KEY, clusterId);
                }
            });
        } catch (JedisException je) {
            LOG.warn("[RedisSessionIdManager] storeClusterId - redis exception", je);
            return false;
        }
    }

    @Override
    protected List<String> scavenge(final List<String> clusterIds) {
        try {
            Set<String> activeIds = getActiveSessionIdsAndDeleteExpired();
            List<String> expiredIds = new LinkedList<String>();
            for (String clusterId : clusterIds) {
                boolean active = activeIds.contains(clusterId);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[RedisSessionIdManager] clusterId: {}, active: {}", clusterId, active);
                }
                if (!active) {
                    expiredIds.add(clusterId);
                }
            }
            if (LOG.isDebugEnabled() && !expiredIds.isEmpty()) {
                LOG.debug("[RedisSessionIdManager] Scavenger found {} sessions to expire: {}", expiredIds.size(),
                        expiredIds);
            }
            return expiredIds;
        } catch (JedisException je) {
            LOG.warn("[RedisSessionIdManager] storeClusterId - redis exception", je);
            return Collections.emptyList();
        }
    }

    private Set<String> getActiveSessionIdsAndDeleteExpired() {
        // 전체 sessionId 리스트를 얻어온다
        Set<String> sessionIdSet = jedisExecutor.execute(new JedisCallback<Set<String>>() {
            @Override
            public Set<String> execute(Jedis jedis) {
                return jedis.smembers(REDIS_SESSIONS_KEY);
            }
        });
        // 각 sessionId에 대해서 session이 존재하는지 여부를 조사한다
        final List<String> sessionIdList = new ArrayList<String>(sessionIdSet);
        List<Object> status = jedisExecutor.execute(new JedisCallback<List<Object>>() {
            @Override
            public List<Object> execute(Jedis jedis) {
                Transaction t = jedis.multi();
                for (String sessionId : sessionIdList) {
                    t.exists(REDIS_SESSION_KEY + sessionId);
                }
                return t.exec();
            }
        });
        // session이 존재하지 않는 sessionId를 expiredSessionIds에 넣는다
        final Set<String> activeSessionIds = new HashSet<String>();
        final List<String> expiredSessionIds = new ArrayList<String>();
        for (int i = 0; i < status.size(); i++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[RedisSessionIdManager] session status sessionId: {}, status: {}",
                        sessionIdList.get(i), status.get(i));
            }
            if (Boolean.TRUE.equals(status.get(i))) {
                activeSessionIds.add(sessionIdList.get(i));
            } else {
                expiredSessionIds.add(sessionIdList.get(i));
            }
        }
        
        if (LOG.isDebugEnabled() && !expiredSessionIds.isEmpty()) {
            LOG.debug("[RedisSessionIdManager] found {} sessionIds to delete: {}", 
                    expiredSessionIds.size(), expiredSessionIds);
        }
        
        if (!expiredSessionIds.isEmpty()) {
            Long deleted = jedisExecutor.execute(new JedisCallback<Long>() {
                @Override
                public Long execute(Jedis jedis) {
                    // TODO Auto-generated method stub
                    return jedis.srem(REDIS_SESSIONS_KEY, expiredSessionIds.toArray(new String[expiredSessionIds.size()]));
                }
            });

            if (LOG.isDebugEnabled()) {
                 LOG.debug("[RedisSessionIdManager] Deleted {} expired sessionIds", deleted);
            }
        }
        return activeSessionIds;
    }

}
