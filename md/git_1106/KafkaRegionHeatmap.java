package work115;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

        // 1️⃣ 读取 Kafka
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

        // 2️⃣ 解析 JSON 并提取 IP 和时间戳
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("gis") && json.getJSONObject("gis").containsKey("ip") && json.containsKey("ts"));

        // 3️⃣ 分配事件时间
        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) ->
                                        element.getDouble("ts").longValue())
                );

        // 4️⃣ 解析 IP → 省份，并按天汇总热力情况
        withWatermark
                .map(value -> {
                    String ip = value.getJSONObject("gis").getString("ip");
                    String region = getRegionByIp(ip);
                    long ts = value.getDouble("ts").longValue();
                    String date = DATE_FORMATTER.format(Instant.ofEpochMilli(ts));
                    JSONObject result = new JSONObject();
                    result.put("date", date);
                    result.put("province", region);
                    return result;
                })
                .keyBy(json -> json.getString("date"))
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    private MapState<String, Long> provinceCountState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        provinceCountState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("province-count", String.class, Long.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        String province = value.getString("province");
                        provinceCountState.put(province, provinceCountState.contains(province)
                                ? provinceCountState.get(province) + 1
                                : 1L);

                        // 注册当日 23:59:59 定时输出
                        LocalDate day = LocalDate.parse(value.getString("date"));
                        long timerTs = day.atTime(23, 59, 59)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                        ctx.timerService().registerEventTimeTimer(timerTs);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        Map<String, Long> resultMap = new HashMap<>();
                        for (String province : provinceCountState.keys()) {
                            resultMap.put(province, provinceCountState.get(province));
                        }

                        // 排序展示热力结果
                        List<Map.Entry<String, Long>> sorted = resultMap.entrySet().stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .collect(Collectors.toList());

                        String day = ctx.getCurrentKey();
                        out.collect("📅 日期：" + day + " 全国热力情况：" + sorted);

                        provinceCountState.clear(); // 清理状态，下一天重新累计
                    }
                })
                .print();

        env.execute("Flink Region Heatmap (IP -> Province)");
    }

    /**
     * 根据 IP 查询地理位置（省份）
     */
    private static String getRegionByIp(String ip) {
        try {
            byte[] cBuff = Searcher.loadContentFromFile(XDB_PATH);
            Searcher searcher = Searcher.newWithBuffer(cBuff);
            String result = searcher.search(ip);
            searcher.close();

            // 格式一般类似：中国|0|北京市|北京市|电信
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
