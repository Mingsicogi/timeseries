package mins.study.ts.redis;


import com.redislabs.redistimeseries.Aggregation;
import com.redislabs.redistimeseries.RedisTimeSeries;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static mins.study.ts.utils.RandomUtil.randomNumber;

@Slf4j
@Service
@RequiredArgsConstructor
public class HumiditySensorDataReporter implements ReportSchedule {

    private final RedisCommands<String, String> redisCommands;
    private final RedissonClient redissonClient;
    private final RedisTimeSeries redisTimeSeries;

    public static final List<String> COUNTRY_LIST = Arrays.asList("US", "KR", "JP");

    private static final String SENSOR = "sensor";

    private static final String HUMIDITY = "humidity";
    private static final String TEMPERATURE = "temperature";

    private static final String ALL = "all";

    private static final Long RETENTION = 60_000L;

    @PostConstruct
    public void init() {
        int index = 0;
        for (String country : COUNTRY_LIST) {
            try {
                Map<String, String> labels = new HashMap<>();
                labels.put("country", country);
                labels.put("sensorType", HUMIDITY);
                redisTimeSeries.create(SENSOR + ":" + index, RETENTION, labels);
                redisTimeSeries.create(SENSOR + ":" + index + "-avg", RETENTION, null);
                redisTimeSeries.createRule(SENSOR + ":" + index, Aggregation.AVG, 60 /*1min*/, SENSOR + ":" + index + "-avg");

            } catch (JedisDataException e) {
                if ("ERR TSDB: key already exists".equals(e.getMessage())){
                    //pass
                    log.info("### already exists ###");
                }
            }

            index++;
        }
    }

    @Override
    @Scheduled(fixedRate = 1000)
    public void loadData() {
        for (int i = 0; i < COUNTRY_LIST.size(); i++) {
            redisTimeSeries.add(SENSOR + ":" + i, randomNumber(10 * (i + 1), 20 * (i + 1)));
        }
    }
}
