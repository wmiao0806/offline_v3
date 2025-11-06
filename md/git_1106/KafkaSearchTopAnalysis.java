package work115;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaSearchTopAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-daily-search";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

        // 1️⃣ 读取 Kafka（历史 + 当天）
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

        // 2️⃣ 转 JSON 并提取 ts + keywords
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("ts") && json.containsKey("keywords"));

        // 3️⃣ 分配事件时间
        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) ->
                                        element.getDouble("ts").longValue())
                );

        // 4️⃣ 按天分组，统计当天搜索词
        withWatermark
                .keyBy(json -> {
                    long ts = json.getDouble("ts").longValue();
                    LocalDate day = Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate();
                    return day.toString(); // yyyy-MM-dd
                })
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    // 当天所有搜索词列表
                    private ListState<String> keywordState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        keywordState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("keyword-state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        JSONArray keywords = value.getJSONArray("keywords");
                        if (keywords != null) {
                            for (int i = 0; i < keywords.size(); i++) {
                                keywordState.add(keywords.getString(i));
                            }
                        }

                        // 注册定时器在当天 23:59:59 输出 TOP10
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
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<String> allKeywords = new ArrayList<>();
                        for (String kw : keywordState.get()) {
                            allKeywords.add(kw);
                        }

                        // 统计词频
                        Map<String, Long> freqMap = allKeywords.stream()
                                .collect(Collectors.groupingBy(k -> k, Collectors.counting()));

                        // 排序取 TOP10
                        List<Map.Entry<String, Long>> top10 = freqMap.entrySet().stream()
                                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
                                .limit(10)
                                .collect(Collectors.toList());

                        String dayStr = ctx.getCurrentKey();
                        out.collect(String.format("日期：%s, 当日搜索词TOP10：%s", dayStr, top10));
                        keywordState.clear();
                    }
                })
                .print();

        env.execute("Flink Daily Search TOP10 Analysis");
    }
}