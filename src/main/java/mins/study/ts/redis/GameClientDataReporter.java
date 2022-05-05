package mins.study.ts.redis;


import com.redislabs.redistimeseries.Aggregation;
import com.redislabs.redistimeseries.RedisTimeSeries;
import com.redislabs.redistimeseries.Value;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static mins.study.ts.utils.RandomUtil.randomNumber;

@Slf4j
@Service
@RequiredArgsConstructor
public class GameClientDataReporter implements ReportSchedule {

    private final RedisCommands<String, String> redisCommands;
    private final RedissonClient redissonClient;
    private final RedisTimeSeries redisTimeSeries;

    public static final List<String> GAME_LIST = Arrays.asList("UD", "EH", "UWO");
    public static final List<Double> WEIGHT_OF_GAME = Arrays.asList(1.0, 2.0, 1.1, 2.6, 2.5, 2.3, 2.2, 2.1, 1.9, 1.8, 1.7, 1.6, 1.5, 1.4, 1.3, 1.2, 0.1, 0.2, 0.4, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.3, 0.2);

    public static Map<String, AtomicInteger> CCU_OF_GAME = Map.of("UD", new AtomicInteger(50), "EH", new AtomicInteger(50), "UWO", new AtomicInteger(50));

    private static final String GAME = "gameCd";

    private static final String CCU = "ccu";
    private static final String TEMPERATURE = "temperature";

    private static final String ALL = "all";

    private static final Long RETENTION = 60_000L;

    private static final String DATA_FORMAT = "{\"value\":\"%s\",\"gameCd\":\"%s\"}";

    @PostConstruct
    public void init() {
        int index = 0;
        for (String gameCd : GAME_LIST) {
            try {
                Map<String, String> labels = new HashMap<>();
                labels.put("gameCd", gameCd);
                labels.put("type", CCU);
                redisTimeSeries.create(GAME + ":" + index, RETENTION, labels); // TSDB
                redisTimeSeries.create(GAME + ":" + index + "-avg", RETENTION, null); // for aggregation
                redisTimeSeries.createRule(GAME + ":" + index, Aggregation.AVG, 60 /*1min*/, GAME + ":" + index + "-avg");

            } catch (JedisDataException e) {
                if ("ERR TSDB: key already exists".equals(e.getMessage())) {
                    //pass
                    log.info("### already exists ###");
                    Value value = redisTimeSeries.get(GAME + ":" + index);
                    CCU_OF_GAME.get(gameCd).set((int)value.getValue());
                }
            }

            index++;
        }
    }

    static int scheduleLoopCount = 0;

    @Override
    @Scheduled(fixedRate = 100)
    public void loadData() {
        scheduleLoopCount++;
        for (int i = 0; i < GAME_LIST.size(); i++) {
            String gameCd = GAME_LIST.get(i);
            AtomicInteger currentCcu = CCU_OF_GAME.get(gameCd);
            int newCcu;
            if (scheduleLoopCount % 600 == 0) {
                newCcu = currentCcu.addAndGet(-(currentCcu.get()/2));
            } else {
                int ccuOfGame = (int)((double)randomNumber(1, 10) * WEIGHT_OF_GAME.get(randomNumber(0, WEIGHT_OF_GAME.size() - 1))) + 1;
                if (Integer.parseInt(RandomStringUtils.randomNumeric(1)) % 2 == 0) {
                    newCcu = currentCcu.addAndGet(ccuOfGame*2);
                } else {
                    newCcu = currentCcu.addAndGet(-(ccuOfGame/2));
                }

            }

            redisTimeSeries.add(GAME + ":" + i, System.currentTimeMillis(), newCcu, Map.of("gameCd", gameCd, "type", CCU));
        }
    }
}
