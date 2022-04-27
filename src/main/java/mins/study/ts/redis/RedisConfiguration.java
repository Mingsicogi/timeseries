package mins.study.ts.redis;

import com.redislabs.redistimeseries.RedisTimeSeries;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class RedisConfiguration {

    @Bean
    public RedisClient redisClient(@Value("${spring.redis.host}") String host, @Value("${spring.redis.port}") int port,  @Value("${spring.redis.database}") int database) {
        return RedisClient.create(RedisURI.builder().withHost(host).withPort(port).withDatabase(database).build());
    }

    @Bean
    public RedisCommands<String, String> redisCommands(RedisClient redisClient) {
        return redisClient.connect().sync();
    }

    @Bean
    public RedissonClient redissonClient(@Value("${spring.redis.host}") String host, @Value("${spring.redis.port}") int port,  @Value("${spring.redis.database}") int database) throws IOException {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + host + ":" + port).setDatabase(database);
        return Redisson.create(config);
    }

    @Bean
    public RedisTimeSeries redisTimeSeries(@Value("${spring.redis.host}") String host, @Value("${spring.redis.port}") int port,  @Value("${spring.redis.database}") int database) {
        RedisTimeSeries redisTimeSeries = new RedisTimeSeries(host, port);
        return redisTimeSeries;
    }
}
