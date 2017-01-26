# Jetty session clustering with REDIS

## Build instruction:

    git clone this repo
    cd jetty-session-redis
    mvn package



## Controlling session serialization

By default, session attributes are serialized using XStream, but this is clearly the worst serializer and you must make sure that you configure the serializer according to your needs.
If you have small sessions with simple types, consider the <strong>JsonSerializer</strong>. If you have complexe objects but all serializable, you can consider the <strong>JbossSerializer</strong>.
You can also create your own ones byt implementing the <strong>Serializer</strong> class of a provided skeleton (<a href="https://github.com/Ovea/jetty-session-redis/tree/master/src/main/java/com/ovea/jetty/session/serializer">see examples here</a>).

Here is the list of provided Serializer:

* com.ovea.jetty.session.serializer.JsonSerializer
* com.ovea.jetty.session.serializer.JdkSerializer
* com.ovea.jetty.session.serializer.XStreamSerializer
* com.ovea.jetty.session.serializer.JBossSerializer


## Debugging

If you need to troobleshoot something, you can put Jetty in DEBUG mode and see the traces from RedisSessionManager and RedisSessionIdManager.

Also, with Redis you have the ability to monitor all the calls. Simply issue in a command-line:

    redis-cli monitor

To see all Redis requests going to the Redis server. If you are using a String serializer such as XStream of Json, you'll be able to see all your session attributes into.

