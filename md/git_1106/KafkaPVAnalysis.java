package work115;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

    // 静态 DateTimeFormatter，用于格式化日期
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用内存状态
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

        // 2️⃣ 转 JSON 并过滤字段
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> json.containsKey("log_type") && json.containsKey("ts"));

        // 3️⃣ 分配事件时间和 watermark
        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (element, recordTimestamp) -> element.getLong("ts"))
                );

        // 4️⃣ 按日期 + 页面分组，统计每日 PV
        withWatermark
                .keyBy(json -> DATE_FORMATTER.format(Instant.ofEpochMilli(json.getLong("ts"))) + "-" + json.getString("log_type"))
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    private ValueState<Long> pvState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        pvState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("daily-pv-state", Long.class, 0L)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        long count = pvState.value() + 1;
                        pvState.update(count);

                        String day = DATE_FORMATTER.format(Instant.ofEpochMilli(value.getLong("ts")));
                        String logType = value.getString("log_type");
                        out.collect(String.format(
                                "日期：%s, 日志类型：%s, 当日累计 PV=%d",
                                day, logType, count
                        ));
                    }
                })
                .print();

        env.execute("Flink Daily PV Analysis");
    }
}
