package work1107;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.PreparedStatement;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaSearchTopAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-daily-search";

    // MySQL 配置
    private static final String MYSQL_URL = "jdbc:mysql://192.168.200.32:3306/work"
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "root";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        bootServerList,
                        kafkaTopic,
                        consumerGroup,
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("ts") && json.containsKey("keywords"));

        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) ->
                                        element.getDouble("ts").longValue())
                );

        withWatermark
                .keyBy(json -> {
                    long ts = json.getDouble("ts").longValue();
                    LocalDate day = Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate();
                    return day.toString(); // yyyy-MM-dd
                })
                .process(new KeyedProcessFunction<String, JSONObject, Tuple2<String, String>>() {

                    private ListState<String> keywordState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        keywordState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("keyword-state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        JSONArray keywords = value.getJSONArray("keywords");
                        if (keywords != null) {
                            for (int i = 0; i < keywords.size(); i++) {
                                keywordState.add(keywords.getString(i));
                            }
                        }

                        LocalDate day = Instant.ofEpochMilli(value.getDouble("ts").longValue())
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();
                        long timerTs = day.atTime(23, 59, 59)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                        ctx.timerService().registerEventTimeTimer(timerTs);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
                        List<String> allKeywords = new ArrayList<>();
                        for (String kw : keywordState.get()) {
                            allKeywords.add(kw);
                        }

                        Map<String, Long> freqMap = allKeywords.stream()
                                .collect(Collectors.groupingBy(k -> k, Collectors.counting()));

                        List<Map.Entry<String, Long>> top10 = freqMap.entrySet().stream()
                                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                                .limit(10)
                                .collect(Collectors.toList());

                        // 转成 JSON 存入 MySQL
                        JSONObject topJson = new JSONObject();
                        for (Map.Entry<String, Long> entry : top10) {
                            topJson.put(entry.getKey(), entry.getValue());
                        }

                        String dayStr = ctx.getCurrentKey();
                        out.collect(Tuple2.of(dayStr, topJson.toJSONString()));
                        keywordState.clear();
                    }
                })
                // 写入 MySQL
                .addSink(
                        JdbcSink.sink(
                                "INSERT INTO daily_search_top10(stat_date, top_keywords) VALUES (?, ?) " +
                                        "ON DUPLICATE KEY UPDATE top_keywords = VALUES(top_keywords)",
                                (PreparedStatement ps, Tuple2<String, String> record) -> {
                                    ps.setString(1, record.f0);
                                    ps.setString(2, record.f1);
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(500)
                                        .withBatchIntervalMs(200)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(MYSQL_URL)
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername(MYSQL_USER)
                                        .withPassword(MYSQL_PASS)
                                        .build()
                        )
                );

        env.execute("Flink Daily Search TOP10 Analysis");
    }
}
