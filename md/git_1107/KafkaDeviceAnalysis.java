package work1107;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 每日设备统计分析
 * 统计结构：按 plat | brand | platv 统计设备数
 * 输出到 MySQL 表 device_daily_stat
 */
public class KafkaDeviceAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-device-daily";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @SneakyThrows
    public static void main(String[] args) {

        // 1️⃣ 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

        // 2️⃣ 读取 Kafka 源
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

        // 3️⃣ 解析 JSON
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("ts") && json.containsKey("device"));

        // 4️⃣ 事件时间 + 水位线
        DataStream<JSONObject> withWatermark = jsonStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) ->
                                element.getDouble("ts").longValue())
        );

        // 5️⃣ 按日期分组统计
        DataStream<DeviceStat> deviceStats = withWatermark
                .keyBy(json -> {
                    long ts = json.getDouble("ts").longValue();
                    LocalDate day = Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate();
                    return day.toString();
                })
                .process(new DeviceDailyStatProcess());

        // 控制台打印
        deviceStats.print("✅ Device Stat:");

        // 6️⃣ 写入 MySQL
        deviceStats.addSink(JdbcSink.sink(
                "INSERT INTO device_daily_stat (stat_date, plat, brand, platv, device_count, log_content) " +
                        "VALUES (?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "device_count = VALUES(device_count), " +
                        "log_content = VALUES(log_content)",

                (ps, stat) -> {
                    ps.setString(1, stat.statDate);
                    ps.setString(2, stat.plat);
                    ps.setString(3, stat.brand);
                    ps.setString(4, stat.platv);
                    ps.setLong(5, stat.count);
                    ps.setString(6, stat.logContent);
                },

                JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(3000)
                        .withMaxRetries(3)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.200.32:3306/work?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute("Flink Device Daily Analysis to MySQL");
    }

    // ✅ 设备统计对象
    public static class DeviceStat {
        String statDate;
        String plat;
        String brand;
        String platv;
        long count;
        String logContent;

        public DeviceStat(String statDate, String plat, String brand, String platv, long count, String logContent) {
            this.statDate = statDate;
            this.plat = plat;
            this.brand = brand;
            this.platv = platv;
            this.count = count;
            this.logContent = logContent;
        }

        @Override
        public String toString() {
            return String.format("[%s] %s | %s | %s → %d", statDate, plat, brand, platv, count);
        }
    }

    // ✅ 按天统计设备使用情况
    public static class DeviceDailyStatProcess extends KeyedProcessFunction<String, JSONObject, DeviceStat> {

        private MapState<String, Long> deviceCountState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            deviceCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("device-count", String.class, Long.class)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<DeviceStat> out) throws Exception {
            JSONObject device = value.getJSONObject("device");
            if (device == null) return;

            String plat = device.getString("plat");
            String brand = device.getString("brand");
            String version = device.getString("platv");

            if (plat == null || brand == null || version == null) return;

            String key = plat + "|" + brand + "|" + version;
            long count = deviceCountState.contains(key) ? deviceCountState.get(key) : 0L;
            deviceCountState.put(key, count + 1);

            // 注册当天 23:59:59 的定时器
            long ts = value.getDouble("ts").longValue();
            LocalDate day = Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate();
            long timerTs = day.atTime(23, 59, 59)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            ctx.timerService().registerEventTimeTimer(timerTs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<DeviceStat> out) throws Exception {
            Map<String, Long> map = new HashMap<>();
            for (String key : deviceCountState.keys()) {
                map.put(key, deviceCountState.get(key));
            }

            // 构造嵌套结构 {"android":{"redmi":{"15":20}}}
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

            for (Map.Entry<String, Map<String, Map<String, Long>>> platEntry : nested.entrySet()) {
                String plat = platEntry.getKey();
                for (Map.Entry<String, Map<String, Long>> brandEntry : platEntry.getValue().entrySet()) {
                    String brand = brandEntry.getKey();
                    for (Map.Entry<String, Long> versionEntry : brandEntry.getValue().entrySet()) {
                        String platv = versionEntry.getKey();
                        long count = versionEntry.getValue();
                        out.collect(new DeviceStat(day, plat, brand, platv, count, JSONObject.toJSONString(nested)));
                    }
                }
            }

            deviceCountState.clear();
        }
    }
}
