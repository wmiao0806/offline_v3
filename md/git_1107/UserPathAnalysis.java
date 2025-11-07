package work1107;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class UserPathAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-user-path-analysis";

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Set<String> TARGET_LOG_TYPES = new HashSet<>(Arrays.asList(
            "app_launch", "home_page", "search", "product_list",
            "product_detail", "add_cart", "order_confirm", "payment"
    ));

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

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
                .filter(json -> json.containsKey("user_id")
                        && json.containsKey("log_type")
                        && json.containsKey("ts")
                        && TARGET_LOG_TYPES.contains(json.getString("log_type")));

        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>)
                                        (element, recordTimestamp) -> element.getDouble("ts").longValue())
                );

        // 输出 PathAnalysisLog 对象
        DataStream<PathAnalysisLog> resultStream = withWatermark
                .keyBy(json -> json.getString("user_id"))
                .process(new UserPathProcessFunction());

        // 控制台打印
        resultStream.map(PathAnalysisLog::toString).print();

        // 写入 MySQL
        resultStream.addSink(JdbcSink.sink(
                "INSERT INTO user_path_analysis_log " +
                        "(user_id, analysis_date, path_sequence, conversion_status, event_count, total_duration, user_type, log_content) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, log) -> {
                    ps.setString(1, log.userId);
                    ps.setString(2, log.analysisDate);
                    ps.setString(3, log.pathSequence);
                    ps.setString(4, log.conversionStatus);
                    ps.setInt(5, log.eventCount);
                    ps.setLong(6, log.totalDuration);
                    ps.setString(7, log.userType);
                    ps.setString(8, log.logContent);
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5000)
                        .withBatchSize(200)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.200.32:3306/work?characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                        .build()
        ));

        env.execute("Flink User Path Analysis - Historical + Current Day");
    }

    static class UserPathProcessFunction extends KeyedProcessFunction<String, JSONObject, PathAnalysisLog> {

        private MapState<String, List<PathEvent>> historicalPathState;
        private ValueState<List<PathEvent>> currentDayPathState;
        private ValueState<String> currentDateState;
        private ValueState<Long> sessionStartTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            historicalPathState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("historical-path", String.class,
                            (Class<List<PathEvent>>) (Class<?>) List.class)
            );
            currentDayPathState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("current-day-path",
                            (Class<List<PathEvent>>) (Class<?>) List.class)
            );
            currentDateState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("current-date", String.class)
            );
            sessionStartTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("session-start-time", Long.class)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<PathAnalysisLog> out) throws Exception {
            String userId = ctx.getCurrentKey();
            long eventTime = value.getDouble("ts").longValue();
            LocalDateTime eventDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(eventTime), ZoneId.systemDefault());
            String eventDate = eventDateTime.format(DATE_FORMATTER);
            String currentDate = currentDateState.value();

            PathEvent event = new PathEvent(
                    value.getString("log_type"),
                    eventTime,
                    eventDateTime.format(TIME_FORMATTER),
                    value.getString("product_id"),
                    value.getString("order_id")
            );

            if (currentDate == null || !currentDate.equals(eventDate)) {
                if (currentDate != null && currentDayPathState.value() != null) {
                    historicalPathState.put(currentDate, currentDayPathState.value());
                    outputDailyPathAnalysis(userId, currentDate, currentDayPathState.value(), out);
                }
                currentDateState.update(eventDate);
                currentDayPathState.update(new ArrayList<>());
                sessionStartTimeState.update(eventTime);
            }

            List<PathEvent> currentPath = currentDayPathState.value();
            if (currentPath == null) currentPath = new ArrayList<>();
            currentPath.add(event);
            currentDayPathState.update(currentPath);

            Long sessionStart = sessionStartTimeState.value();
            if (sessionStart != null && eventTime - sessionStart > 30 * 60 * 1000) {
                sessionStartTimeState.update(eventTime);
            }

            detectAnomalies(userId, currentPath, event, out);

            long endOfDayTimer = eventDateTime.toLocalDate()
                    .atTime(23, 59, 59)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            ctx.timerService().registerEventTimeTimer(endOfDayTimer);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PathAnalysisLog> out) throws Exception {
            String userId = ctx.getCurrentKey();
            String currentDate = currentDateState.value();
            List<PathEvent> currentPath = currentDayPathState.value();

            if (currentPath != null && !currentPath.isEmpty()) {
                historicalPathState.put(currentDate, currentPath);
                outputComprehensiveAnalysis(userId, currentDate, currentPath, out);
                currentDayPathState.clear();
            }
        }

        private void detectAnomalies(String userId, List<PathEvent> path,
                                     PathEvent currentEvent, Collector<PathAnalysisLog> out) {
            if (path.size() < 2) return;

            long recentSameActionCount = path.stream()
                    .filter(e -> e.action.equals(currentEvent.action))
                    .filter(e -> currentEvent.timestamp - e.timestamp < 60 * 1000)
                    .count();

            if (recentSameActionCount > 5) {
            }

            PathEvent lastEvent = path.get(path.size() - 2);
        }

        private void outputDailyPathAnalysis(String userId, String date,
                                             List<PathEvent> path, Collector<PathAnalysisLog> out) throws Exception {
            if (path == null || path.isEmpty()) return;
            outputComprehensiveAnalysis(userId, date, path, out);
        }

        private void outputComprehensiveAnalysis(String userId, String currentDate,
                                                 List<PathEvent> currentPath,
                                                 Collector<PathAnalysisLog> out) throws Exception {
            String pathSequence = currentPath.stream()
                    .map(e -> e.action)
                    .collect(Collectors.joining(" → "));
            boolean todayConversion = currentPath.stream().anyMatch(e -> "payment".equals(e.action));
            String conversionStatus = todayConversion ? "已转化" : "未转化";
            long duration = currentPath.get(currentPath.size() - 1).timestamp - currentPath.get(0).timestamp;

            int historicalConversions = 0;
            for (Map.Entry<String, List<PathEvent>> entry : historicalPathState.entries()) {
                List<PathEvent> dayPath = entry.getValue();
                if (dayPath.stream().anyMatch(e -> "payment".equals(e.action))) historicalConversions++;
            }

            String userType;
            if (historicalConversions > 0 && todayConversion) userType = "高价值用户";
            else if (historicalConversions == 0 && !todayConversion) userType = "普通用户";
            else userType = "观望用户";

            StringBuilder sb = new StringBuilder();
            sb.append("📊 用户路径综合分析报告\n");
            sb.append("用户ID: ").append(userId).append("\n");
            sb.append("日期: ").append(currentDate).append("\n");
            sb.append("路径: ").append(pathSequence).append("\n");
            sb.append("转化状态: ").append(conversionStatus).append("\n");
            sb.append("用户类型: ").append(userType).append("\n");
            sb.append("时长: ").append(duration / 1000).append("秒\n");

            out.collect(new PathAnalysisLog(
                    userId,
                    currentDate,
                    pathSequence,
                    conversionStatus,
                    currentPath.size(),
                    duration,
                    userType,
                    sb.toString()
            ));
        }
    }

    static class PathEvent implements java.io.Serializable {
        String action;
        long timestamp;
        String timeStr;
        String productId;
        String orderId;

        public PathEvent(String action, long timestamp, String timeStr,
                         String productId, String orderId) {
            this.action = action;
            this.timestamp = timestamp;
            this.timeStr = timeStr;
            this.productId = productId;
            this.orderId = orderId;
        }
    }

    public static class PathAnalysisLog {
        String userId;
        String analysisDate;
        String pathSequence;
        String conversionStatus;
        int eventCount;
        long totalDuration;
        String userType;
        String logContent;

        public PathAnalysisLog(String userId, String analysisDate, String pathSequence,
                               String conversionStatus, int eventCount, long totalDuration,
                               String userType, String logContent) {
            this.userId = userId;
            this.analysisDate = analysisDate;
            this.pathSequence = pathSequence;
            this.conversionStatus = conversionStatus;
            this.eventCount = eventCount;
            this.totalDuration = totalDuration;
            this.userType = userType;
            this.logContent = logContent;
        }

        @Override
        public String toString() {
            return logContent;
        }
    }
}
