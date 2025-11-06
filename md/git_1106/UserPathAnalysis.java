package work115;

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

/**
 * 用户路径分析：历史天 + 当天
 * 功能：
 * 1. 追踪每个用户的完整行为路径
 * 2. 分析转化漏斗和流失节点
 * 3. 识别异常行为模式
 * 4. 输出高价值转化路径
 */
public class UserPathAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-user-path-analysis";

    private static final DateTimeFormatter DATE_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 定义行为事件类型
    private static final Set<String> TARGET_LOG_TYPES = new HashSet<>(Arrays.asList(
            "app_launch", "home_page", "search", "product_list", 
            "product_detail", "add_cart", "order_confirm", "payment"
    ));

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new MemoryStateBackend(true));

        // 1️⃣ 读取 Kafka（从最早开始，包含历史数据）
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

        // 2️⃣ 解析 JSON 并过滤有效数据
        DataStream<JSONObject> jsonStream = kafkaSource
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(JSONObject::parseObject)
                .filter(json -> {
                    // 必须包含 user_id, log_type, ts
                    return json.containsKey("user_id") 
                            && json.containsKey("log_type") 
                            && json.containsKey("ts")
                            && TARGET_LOG_TYPES.contains(json.getString("log_type"));
                });

        // 3️⃣ 分配事件时间和水位线
        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) 
                                        (element, recordTimestamp) -> element.getDouble("ts").longValue())
                );

        // 4️⃣ 按 user_id 分组，分析用户路径
        withWatermark
                .keyBy(json -> json.getString("user_id"))
                .process(new UserPathProcessFunction())
                .print();

        env.execute("Flink User Path Analysis - Historical + Current Day");
    }

    /**
     * 用户路径处理函数
     */
    static class UserPathProcessFunction extends KeyedProcessFunction<String, JSONObject, String> {

        // 存储用户的历史行为路径（按日期组织）
        private MapState<String, List<PathEvent>> historicalPathState;
        
        // 存储当天的行为路径
        private ValueState<List<PathEvent>> currentDayPathState;
        
        // 存储当前处理的日期
        private ValueState<String> currentDateState;
        
        // 存储会话开始时间（用于超时判断）
        private ValueState<Long> sessionStartTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 历史路径状态：Map<日期, 路径列表>
            historicalPathState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("historical-path", String.class, 
                            (Class<List<PathEvent>>) (Class<?>) List.class)
            );
            
            // 当天路径状态
            currentDayPathState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("current-day-path", 
                            (Class<List<PathEvent>>) (Class<?>) List.class)
            );
            
            // 当前日期状态
            currentDateState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("current-date", String.class)
            );
            
            // 会话开始时间
            sessionStartTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("session-start-time", Long.class)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
            String userId = ctx.getCurrentKey();
            long eventTime = value.getDouble("ts").longValue();
            LocalDateTime eventDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(eventTime), ZoneId.systemDefault());
            String eventDate = eventDateTime.format(DATE_FORMATTER);
            String currentDate = currentDateState.value();

            // 构造路径事件
            PathEvent event = new PathEvent(
                    value.getString("log_type"),
                    eventTime,
                    eventDateTime.format(TIME_FORMATTER),
                    value.getString("product_id"),
                    value.getString("order_id")
            );

            // 判断是否是新的一天
            if (currentDate == null || !currentDate.equals(eventDate)) {
                // 如果有当天数据，保存到历史
                if (currentDate != null && currentDayPathState.value() != null) {
                    historicalPathState.put(currentDate, currentDayPathState.value());
                    
                    // 输出前一天的路径分析
                    outputDailyPathAnalysis(userId, currentDate, 
                            currentDayPathState.value(), out);
                }
                
                // 重置当天状态
                currentDateState.update(eventDate);
                currentDayPathState.update(new ArrayList<>());
                sessionStartTimeState.update(eventTime);
            }

            // 添加事件到当天路径
            List<PathEvent> currentPath = currentDayPathState.value();
            if (currentPath == null) {
                currentPath = new ArrayList<>();
            }
            currentPath.add(event);
            currentDayPathState.update(currentPath);

            // 会话超时检测（30分钟无操作视为新会话）
            Long sessionStart = sessionStartTimeState.value();
            if (sessionStart != null && eventTime - sessionStart > 30 * 60 * 1000) {
                out.collect(String.format("⚠️ 用户 %s 会话超时，新会话开始", userId));
                sessionStartTimeState.update(eventTime);
            }

            // 实时异常检测
            detectAnomalies(userId, currentPath, event, out);

            // 注册定时器：每天 23:59:59 输出汇总
            long endOfDayTimer = eventDateTime.toLocalDate()
                    .atTime(23, 59, 59)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            ctx.timerService().registerEventTimeTimer(endOfDayTimer);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            String userId = ctx.getCurrentKey();
            String currentDate = currentDateState.value();
            List<PathEvent> currentPath = currentDayPathState.value();

            if (currentPath != null && !currentPath.isEmpty()) {
                // 保存到历史
                historicalPathState.put(currentDate, currentPath);
                
                // 输出完整路径分析
                outputComprehensiveAnalysis(userId, currentDate, currentPath, out);
                
                // 清空当天状态
                currentDayPathState.clear();
            }
        }

        /**
         * 实时异常检测
         */
        private void detectAnomalies(String userId, List<PathEvent> path, 
                                     PathEvent currentEvent, Collector<String> out) {
            if (path.size() < 2) return;

            // 检测1: 短时间内重复访问同一页面
            long recentSameActionCount = path.stream()
                    .filter(e -> e.action.equals(currentEvent.action))
                    .filter(e -> currentEvent.timestamp - e.timestamp < 60 * 1000) // 1分钟内
                    .count();
            
            if (recentSameActionCount > 5) {
                out.collect(String.format(
                        "🚨 异常检测 - 用户: %s, 行为: %s, 1分钟内重复%d次（疑似爬虫）", 
                        userId, currentEvent.action, recentSameActionCount));
            }

            // 检测2: 异常路径（支付后又回到商品详情）
            PathEvent lastEvent = path.get(path.size() - 2);
            if ("payment".equals(lastEvent.action) && "product_detail".equals(currentEvent.action)) {
                out.collect(String.format(
                        "⚠️ 异常路径 - 用户: %s, 支付后返回商品详情（可能存在问题）", userId));
            }

            // 检测3: 订单确认后返回商品详情（犹豫信号）
            if ("order_confirm".equals(lastEvent.action) && "product_detail".equals(currentEvent.action)) {
                out.collect(String.format(
                        "💡 决策犹豫 - 用户: %s, 订单ID: %s, 建议推送优惠券", 
                        userId, lastEvent.orderId));
            }
        }

        /**
         * 输出单日路径分析
         */
        private void outputDailyPathAnalysis(String userId, String date, 
                                            List<PathEvent> path, Collector<String> out) {
            if (path == null || path.isEmpty()) return;

            StringBuilder sb = new StringBuilder();
            sb.append("\n════════════════════════════════════════\n");
            sb.append(String.format("📊 用户路径分析 - %s\n", date));
            sb.append(String.format("用户ID: %s\n", userId));
            sb.append(String.format("事件总数: %d\n", path.size()));
            sb.append("────────────────────────────────────────\n");
            
            // 路径序列
            String pathSequence = path.stream()
                    .map(e -> e.action)
                    .collect(Collectors.joining(" → "));
            sb.append(String.format("路径: %s\n", pathSequence));
            
            // 关键指标
            boolean hasConversion = path.stream().anyMatch(e -> "payment".equals(e.action));
            long duration = path.get(path.size() - 1).timestamp - path.get(0).timestamp;
            sb.append(String.format("转化状态: %s\n", hasConversion ? "✓ 已转化" : "✗ 未转化"));
            sb.append(String.format("总时长: %d秒\n", duration / 1000));
            
            // 详细步骤
            sb.append("────────────────────────────────────────\n");
            sb.append("详细步骤:\n");
            for (int i = 0; i < path.size(); i++) {
                PathEvent e = path.get(i);
                long stepDuration = i > 0 ? 
                        (e.timestamp - path.get(i-1).timestamp) / 1000 : 0;
                sb.append(String.format("  %d. %s [%s] (停留%ds)", 
                        i + 1, e.action, e.timeStr, stepDuration));
                if (e.productId != null) {
                    sb.append(String.format(" - 商品:%s", e.productId));
                }
                if (e.orderId != null) {
                    sb.append(String.format(" - 订单:%s", e.orderId));
                }
                sb.append("\n");
            }
            
            sb.append("════════════════════════════════════════\n");
            out.collect(sb.toString());
        }

        /**
         * 输出综合分析（历史 + 当天）
         */
        private void outputComprehensiveAnalysis(String userId, String currentDate, 
                                                List<PathEvent> currentPath, 
                                                Collector<String> out) throws Exception {
            StringBuilder sb = new StringBuilder();
            sb.append("\n╔════════════════════════════════════════╗\n");
            sb.append("║     用户路径综合分析报告（历史+当天）      ║\n");
            sb.append("╚════════════════════════════════════════╝\n");
            sb.append(String.format("用户ID: %s\n", userId));
            sb.append(String.format("分析日期: %s\n", currentDate));
            sb.append("────────────────────────────────────────\n");

            // 历史数据统计
            int historicalDays = 0;
            int totalHistoricalEvents = 0;
            int historicalConversions = 0;
            
            for (Map.Entry<String, List<PathEvent>> entry : historicalPathState.entries()) {
                historicalDays++;
                List<PathEvent> dayPath = entry.getValue();
                totalHistoricalEvents += dayPath.size();
                if (dayPath.stream().anyMatch(e -> "payment".equals(e.action))) {
                    historicalConversions++;
                }
            }

            sb.append("【历史行为概览】\n");
            sb.append(String.format("  活跃天数: %d天\n", historicalDays));
            sb.append(String.format("  总事件数: %d\n", totalHistoricalEvents));
            sb.append(String.format("  历史转化: %d次\n", historicalConversions));
            sb.append(String.format("  转化率: %.2f%%\n", 
                    historicalDays > 0 ? (historicalConversions * 100.0 / historicalDays) : 0));

            // 当天数据
            sb.append("\n【当天行为分析】\n");
            boolean todayConversion = currentPath.stream()
                    .anyMatch(e -> "payment".equals(e.action));
            sb.append(String.format("  事件数: %d\n", currentPath.size()));
            sb.append(String.format("  转化状态: %s\n", todayConversion ? "✓ 已转化" : "✗ 未转化"));
            
            String todayPath = currentPath.stream()
                    .map(e -> e.action)
                    .collect(Collectors.joining(" → "));
            sb.append(String.format("  路径: %s\n", todayPath));

            // 行为模式分析
            sb.append("\n【行为模式识别】\n");
            Map<String, Long> actionFreq = currentPath.stream()
                    .collect(Collectors.groupingBy(e -> e.action, Collectors.counting()));
            
            // 查找重复最多的行为
            actionFreq.entrySet().stream()
                    .filter(e -> e.getValue() > 2)
                    .forEach(e -> sb.append(String.format("  • %s 重复%d次\n", e.getKey(), e.getValue())));

            // 用户画像
            sb.append("\n【用户画像】\n");
            if (historicalConversions > 0 && todayConversion) {
                sb.append("  类型: 高价值用户（复购用户）\n");
            } else if (historicalDays >= 3 && historicalConversions == 0) {
                sb.append("  类型: 观望用户（需刺激转化）\n");
            } else if (todayConversion && historicalDays == 0) {
                sb.append("  类型: 新用户首单\n");
            } else {
                sb.append("  类型: 普通用户\n");
            }

            sb.append("════════════════════════════════════════\n");
            out.collect(sb.toString());
        }
    }

    /**
     * 路径事件实体
     */
    static class PathEvent implements java.io.Serializable {
        String action;      // 行为类型
        long timestamp;     // 时间戳
        String timeStr;     // 格式化时间
        String productId;   // 商品ID
        String orderId;     // 订单ID

        public PathEvent(String action, long timestamp, String timeStr, 
                        String productId, String orderId) {
            this.action = action;
            this.timestamp = timestamp;
            this.timeStr = timeStr;
            this.productId = productId;
            this.orderId = orderId;
        }
    }
}