# Jetty session clustering with Redis

## Build instruction:

    git clone this repo
    cd jetty-session-redis
    mvn package


## Controlling session serialization

By default, session attributes are serialized using `JsonSerializer`. It is appropriate if you have small sessions with simple types. For more complicated sessions, use other forks with the `JBossSerializer`.
You can also create your own ones by implementing the <strong>Serializer</strong> class of a provided skeleton (<a href="https://github.com/dbaq/jetty-session-redis/tree/master/src/main/java/com/ovea/jetty/session/serializer">see examples here</a>).

Here is the list of provided Serializer:

* com.ovea.jetty.session.serializer.JsonSerializer


## Debugging

If you need to troobleshoot something, you can put Jetty in DEBUG mode and see the traces from RedisSessionManager and RedisSessionIdManager.

Also, with Redis you have the ability to monitor all the calls. Simply issue in a command-line:

    redis-cli monitor

To see all Redis requests going to the Redis server. If you are using a String serializer such as XStream of Json, you'll be able to see all your session attributes into.

