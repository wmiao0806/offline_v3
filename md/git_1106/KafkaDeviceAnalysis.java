package work115;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class KafkaDeviceAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-device-daily";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

        // 1️⃣ 读取 Kafka 源
        DataStream<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        bootServerList,
                        kafkaTopic,
                        consumerGroup,
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 2️⃣ 解析 JSON
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("ts") && json.containsKey("device"));

        // 3️⃣ 事件时间分配
        DataStream<JSONObject> withWatermark = jsonStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) ->
                                element.getDouble("ts").longValue())
        );

        // 4️⃣ 按天分组，统计设备使用情况
        withWatermark
                .keyBy(json -> {
                    long ts = json.getDouble("ts").longValue();
                    LocalDate day = Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate();
                    return day.toString(); // yyyy-MM-dd
                })
                .process(new DeviceDailyStatProcess())
                .print();

        env.execute("Flink Device Daily Analysis");
    }

    // 设备统计处理逻辑
    public static class DeviceDailyStatProcess extends KeyedProcessFunction<String, JSONObject, String> {

        private MapState<String, Long> deviceCountState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            deviceCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("device-count", String.class, Long.class)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
            JSONObject device = value.getJSONObject("device");
            if (device == null) return;

            String plat = device.getString("plat");
            String brand = device.getString("brand");
            String version = device.getString("platv");

            if (plat == null || brand == null || version == null) return;

            String key = plat + "|" + brand + "|" + version;
            long count = deviceCountState.contains(key) ? deviceCountState.get(key) : 0L;
            deviceCountState.put(key, count + 1);

            // 注册定时器在当天 23:59:59 输出统计
            long ts = value.getDouble("ts").longValue();
            LocalDate day = Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate();
            long timerTs = day.atTime(23, 59, 59)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            ctx.timerService().registerEventTimeTimer(timerTs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Map<String, Long> map = new HashMap<>();
            for (String key : deviceCountState.keys()) {
                map.put(key, deviceCountState.get(key));
            }

            // 构造嵌套结构 {"android": {"redmi": {"15": 20}}}
            Map<String, Map<String, Map<String, Long>>> nested = new HashMap<>();
            for (Map.Entry<String, Long> entry : map.entrySet()) {
                String[] parts = entry.getKey().split("\\|");
                if (parts.length != 3) continue;
                nested
                        .computeIfAbsent(parts[0], k -> new HashMap<>())
                        .computeIfAbsent(parts[1], k -> new HashMap<>())
                        .put(parts[2], entry.getValue());
            }

            String day = ctx.getCurrentKey();
            out.collect("📅 日期：" + day + " 设备统计：" + JSONObject.toJSONString(nested));
            deviceCountState.clear();
        }
    }
}
