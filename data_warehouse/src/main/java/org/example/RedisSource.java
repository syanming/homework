package org.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * redis数据源
 *
 * @author ly
 * @date 2023/04/16,星期日,下午 08:25
 */
public class RedisSource implements SourceFunction<HashMap<String, String>> {
    private static final long serialVersionUID = -261321311311769281L;
    private Logger logger = LoggerFactory.getLogger(RedisSource.class);


    private Jedis jedis;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        this.jedis = new Jedis("bigdata02", 6379);
        HashMap<String, String> map = new HashMap<>();
        while (isRunning) {
            try {
                map.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String area = entry.getKey();
                    String value = entry.getValue();
                    String[] fields = value.split(",");
                    for (String country : fields) {
                        map.put(country, area);
                    }
                }
                if (map.size() > 0) {
                    sourceContext.collect(map);
                }
            } catch (JedisConnectionException e) {
                logger.error("redis 连接一场：" + e.getCause());
            } catch (Exception e) {
                logger.error("数据源发生了异常！！");
            }

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null) {
            jedis.close();
        }

    }
}


