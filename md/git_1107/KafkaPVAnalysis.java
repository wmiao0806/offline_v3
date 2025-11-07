package work1107;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class KafkaPVAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-daily-stats";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @SneakyThrows
    public static void main(String[] args) {
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

        // 2️⃣ 解析 JSON
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("log_type") && json.containsKey("ts"));

        // 3️⃣ 分配事件时间
        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                        (element, recordTimestamp) -> element.getLong("ts"))
                );

        // 4️⃣ 分组 + 实时累计
        DataStream<DailyPV> pvStream = withWatermark
                .keyBy(json -> DATE_FORMATTER.format(Instant.ofEpochMilli(json.getLong("ts"))) + "-" + json.getString("log_type"))
                .process(new DailyPVProcessFunction());

        // 控制台输出
        pvStream.map(pv -> String.format(
                "📅 %s | 页面：%s | 当日累计PV：%d",
                pv.statDate, pv.logType, pv.pvCount
        )).print();

        // 5️⃣ 写入 MySQL（带自动更新）
        pvStream.addSink(JdbcSink.sink(
                "INSERT INTO daily_pv_analysis (stat_date, log_type, pv_count) " +
                        "VALUES (?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE pv_count = VALUES(pv_count)",
                (ps, pv) -> {
                    ps.setString(1, pv.statDate);
                    ps.setString(2, pv.logType);
                    ps.setLong(3, pv.pvCount);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(200)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.200.32:3306/work?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute("Flink Daily PV Analysis -> MySQL");
    }

    // ✅ ProcessFunction 输出结构体
    static class DailyPVProcessFunction extends KeyedProcessFunction<String, JSONObject, DailyPV> {
        private transient ValueState<Long> pvState;

        @Override
        public void open(Configuration parameters) {
            pvState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pv-state", Long.class, 0L)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<DailyPV> out) throws Exception {
            long count = pvState.value() + 1;
            pvState.update(count);

            String day = DATE_FORMATTER.format(Instant.ofEpochMilli(value.getLong("ts")));
            String logType = value.getString("log_type");

            out.collect(new DailyPV(day, logType, count));
        }
    }

    // ✅ Java Bean：每日PV统计
    public static class DailyPV {
        public String statDate;
        public String logType;
        public long pvCount;

        public DailyPV(String statDate, String logType, long pvCount) {
            this.statDate = statDate;
            this.logType = logType;
            this.pvCount = pvCount;
        }
    }
}
