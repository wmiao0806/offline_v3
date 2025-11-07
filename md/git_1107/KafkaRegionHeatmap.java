package work1107;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.xdb.Searcher;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaRegionHeatmap {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-region-heatmap";

    private static final String XDB_PATH = "D:\\java\\six\\stream_prod_1027\\stream-realtime\\src\\main\\resources\\ip2region_v4.xdb";
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    // 🚩 定义输出结构
    public static class RegionHeatmapLog {
        public String statDate;
        public String province;
        public long visitCount;
        public String logContent;

        public RegionHeatmapLog(String statDate, String province, long visitCount, String logContent) {
            this.statDate = statDate;
            this.province = province;
            this.visitCount = visitCount;
            this.logContent = logContent;
        }

        @Override
        public String toString() {
            return logContent;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1️⃣ Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

        // 2️⃣ Kafka Source
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

        // 3️⃣ 解析 JSON
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("gis") && json.getJSONObject("gis").containsKey("ip") && json.containsKey("ts"));

        // 4️⃣ 提取时间戳
        DataStream<JSONObject> withWatermark = jsonStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                (element, recordTimestamp) -> element.getDouble("ts").longValue())
        );

        // 5️⃣ IP → 省份，并汇总到 MySQL
        DataStream<RegionHeatmapLog> heatmapStream = withWatermark
                .map(value -> {
                    String ip = value.getJSONObject("gis").getString("ip");
                    String province = getRegionByIp(ip);
                    long ts = value.getDouble("ts").longValue();
                    String date = DATE_FORMATTER.format(Instant.ofEpochMilli(ts));
                    JSONObject json = new JSONObject();
                    json.put("date", date);
                    json.put("province", province);
                    return json;
                })
                .keyBy(json -> json.getString("date"))
                .process(new HeatmapProcessFunction());

        // 控制台输出
        heatmapStream.map(RegionHeatmapLog::toString).print();

        // ✅ 写入 MySQL
        heatmapStream.addSink(JdbcSink.sink(
                "INSERT INTO region_heatmap_daily (stat_date, province, visit_count, log_content) " +
                        "VALUES (?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "visit_count = VALUES(visit_count), " +
                        "log_content = VALUES(log_content)",
                (ps, log) -> {
                    ps.setString(1, log.statDate);
                    ps.setString(2, log.province);
                    ps.setLong(3, log.visitCount);
                    ps.setString(4, log.logContent);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(200)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.200.32:3306/work?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute("Flink Region Heatmap to MySQL");
    }

    // 6️⃣ 热力图统计逻辑
    public static class HeatmapProcessFunction extends KeyedProcessFunction<String, JSONObject, RegionHeatmapLog> {
        private transient MapState<String, Long> provinceCountState;

        @Override
        public void open(Configuration parameters) {
            provinceCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("province-count", String.class, Long.class)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<RegionHeatmapLog> out) throws Exception {
            String province = value.getString("province");
            provinceCountState.put(province,
                    provinceCountState.contains(province)
                            ? provinceCountState.get(province) + 1
                            : 1L);

            LocalDate day = LocalDate.parse(value.getString("date"));
            long timerTs = day.atTime(23, 59, 59)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            ctx.timerService().registerEventTimeTimer(timerTs);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<RegionHeatmapLog> out) throws Exception {
            String date = ctx.getCurrentKey();
            Map<String, Long> result = new HashMap<>();
            for (String province : provinceCountState.keys()) {
                result.put(province, provinceCountState.get(province));
            }

            List<Map.Entry<String, Long>> sorted = result.entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .collect(Collectors.toList());

            String summary = "📅 " + date + " 热力排行：" + sorted;

            for (Map.Entry<String, Long> entry : sorted) {
                out.collect(new RegionHeatmapLog(date, entry.getKey(), entry.getValue(), summary));
            }

            provinceCountState.clear();
        }
    }

    // 7️⃣ IP → 省份
    private static String getRegionByIp(String ip) {
        try {
            byte[] cBuff = Searcher.loadContentFromFile(XDB_PATH);
            Searcher searcher = Searcher.newWithBuffer(cBuff);
            String result = searcher.search(ip);
            searcher.close();

            if (result != null && !result.isEmpty()) {
                String[] parts = result.split("\\|");
                return parts.length >= 3 ? parts[2] : "未知地区";
            }
            return "未知地区";
        } catch (IOException e) {
            return "IP解析失败";
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
