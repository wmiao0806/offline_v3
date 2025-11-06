package work115;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

/**
 * 用户画像分析：登录天数、行为分析、时间段分布
 * 输出格式：JSON -> Elasticsearch
 */
public class UserProfileAnalysis {

    private static final String kafkaTopic = "realtime_v3_logs";
    private static final String bootServerList = "192.168.200.30:9092";
    private static final String consumerGroup = "flink-user-profile-analysis";

    private static final DateTimeFormatter DATE_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
                .filter(json -> json.containsKey("user_id") 
                        && json.containsKey("log_type") 
                        && json.containsKey("ts"));

        // 3️⃣ 分配事件时间
        DataStream<JSONObject> withWatermark = jsonStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) 
                                        (element, recordTimestamp) -> element.getDouble("ts").longValue())
                );

        // 4️⃣ 按 user_id 分组，构建用户画像
        withWatermark
                .keyBy(json -> json.getString("user_id"))
                .process(new UserProfileProcessFunction())
                .print("UserProfile"); // 输出到控制台，实际应写入ES

        // 5️⃣ 可选：集成 Elasticsearch Sink
        // withWatermark.keyBy(...).process(...).sinkTo(elasticsearchSink);

        env.execute("Flink User Profile Analysis -> ES");
    }

    /**
     * 用户画像处理函数
     */
    static class UserProfileProcessFunction extends KeyedProcessFunction<String, JSONObject, String> {

        // 用户画像状态
        private ValueState<UserProfile> profileState;

        @Override
        public void open(Configuration parameters) throws Exception {
            profileState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("user-profile", UserProfile.class)
            );
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
            String userId = ctx.getCurrentKey();
            long eventTime = value.getDouble("ts").longValue();
            LocalDateTime eventDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(eventTime), ZoneId.systemDefault());
            String eventDate = eventDateTime.format(DATE_FORMATTER);
            String logType = value.getString("log_type");

            // 获取或初始化用户画像
            UserProfile profile = profileState.value();
            if (profile == null) {
                profile = new UserProfile(userId);
            }

            // 更新登录天数
            profile.addLoginDate(eventDate);

            // 更新行为标记
            updateBehaviorFlags(profile, value, logType, eventDateTime);

            // 更新时间段分布
            profile.addTimeSlot(eventDateTime.getHour());

            // 保存状态
            profileState.update(profile);

            // 每次更新后输出最新画像（实时更新）
            String profileJson = profile.toJson();
            out.collect(profileJson);

            // 注册定时器：每天凌晨输出汇总画像
            long nextDayTimer = eventDateTime.toLocalDate().plusDays(1)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
            ctx.timerService().registerEventTimeTimer(nextDayTimer);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            UserProfile profile = profileState.value();
            if (profile != null) {
                // 输出每日汇总画像
                String summaryJson = profile.toDailySummaryJson();
                out.collect(summaryJson);
            }
        }

        /**
         * 更新行为标记
         */
        private void updateBehaviorFlags(UserProfile profile, JSONObject value, 
                                        String logType, LocalDateTime eventTime) {
            String eventDate = eventTime.format(DATE_FORMATTER);

            // 搜索行为
            if ("search".equals(logType) && value.containsKey("keywords")) {
                profile.markSearch(eventDate);
                JSONArray keywords = value.getJSONArray("keywords");
                if (keywords != null) {
                    for (int i = 0; i < keywords.size(); i++) {
                        profile.addSearchKeyword(keywords.getString(i));
                    }
                }
            }

            // 购买行为（支付或订单相关）
            if ("payment".equals(logType) || 
                ("order_confirm".equals(logType) && value.containsKey("order_id"))) {
                profile.markPurchase(eventDate);
                if (value.containsKey("order_id")) {
                    profile.addOrderId(value.getString("order_id"));
                }
            }

            // 浏览行为（商品详情、列表等）
            if ("product_detail".equals(logType) || "product_list".equals(logType)) {
                profile.markBrowse(eventDate);
                if (value.containsKey("product_id")) {
                    profile.addBrowsedProduct(value.getString("product_id"));
                }
            }

            // 设备信息（仅记录一次）
            if (profile.deviceInfo == null && value.containsKey("device")) {
                JSONObject device = value.getJSONObject("device");
                profile.setDeviceInfo(
                    device.getString("brand"),
                    device.getString("device"),
                    device.getString("plat"),
                    device.getString("platv")
                );
            }

            // 网络类型统计
            if (value.containsKey("network")) {
                JSONObject network = value.getJSONObject("network");
                profile.addNetworkType(network.getString("net"));
            }
        }
    }

    /**
     * 用户画像实体
     */
    static class UserProfile implements java.io.Serializable {
        String userId;
        Set<String> loginDates;              // 登录日期集合
        Map<String, Boolean> searchDates;    // 搜索日期
        Map<String, Boolean> purchaseDates;  // 购买日期
        Map<String, Boolean> browseDates;    // 浏览日期
        Map<Integer, Integer> timeSlots;     // 时间段分布 (小时 -> 次数)
        Set<String> searchKeywords;          // 搜索关键词
        Set<String> browsedProducts;         // 浏览过的商品
        Set<String> orderIds;                // 订单ID
        Map<String, Integer> networkTypes;   // 网络类型统计
        DeviceInfo deviceInfo;               // 设备信息
        long lastUpdateTime;                 // 最后更新时间

        public UserProfile(String userId) {
            this.userId = userId;
            this.loginDates = new TreeSet<>();
            this.searchDates = new HashMap<>();
            this.purchaseDates = new HashMap<>();
            this.browseDates = new HashMap<>();
            this.timeSlots = new HashMap<>();
            this.searchKeywords = new HashSet<>();
            this.browsedProducts = new HashSet<>();
            this.orderIds = new HashSet<>();
            this.networkTypes = new HashMap<>();
            this.lastUpdateTime = System.currentTimeMillis();
        }

        public void addLoginDate(String date) {
            loginDates.add(date);
            lastUpdateTime = System.currentTimeMillis();
        }

        public void markSearch(String date) {
            searchDates.put(date, true);
        }

        public void markPurchase(String date) {
            purchaseDates.put(date, true);
        }

        public void markBrowse(String date) {
            browseDates.put(date, true);
        }

        public void addTimeSlot(int hour) {
            timeSlots.put(hour, timeSlots.getOrDefault(hour, 0) + 1);
        }

        public void addSearchKeyword(String keyword) {
            if (keyword != null && !keyword.trim().isEmpty()) {
                searchKeywords.add(keyword);
            }
        }

        public void addBrowsedProduct(String productId) {
            if (productId != null && !productId.trim().isEmpty()) {
                browsedProducts.add(productId);
            }
        }

        public void addOrderId(String orderId) {
            if (orderId != null && !orderId.trim().isEmpty()) {
                orderIds.add(orderId);
            }
        }

        public void addNetworkType(String netType) {
            if (netType != null) {
                networkTypes.put(netType, networkTypes.getOrDefault(netType, 0) + 1);
            }
        }

        public void setDeviceInfo(String brand, String model, String plat, String platv) {
            this.deviceInfo = new DeviceInfo(brand, model, plat, platv);
        }

        /**
         * 转为 JSON 格式（实时更新）
         */
        public String toJson() {
            JSONObject json = new JSONObject(true); // 保持顺序
            json.put("user_id", userId);
            json.put("profile_type", "realtime");
            json.put("update_time", LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(lastUpdateTime), 
                    ZoneId.systemDefault()).format(DATETIME_FORMATTER));

            // 登录天数
            json.put("login_days_count", loginDates.size());
            json.put("login_dates", new ArrayList<>(loginDates));

            // 行为分析
            JSONObject behavior = new JSONObject(true);
            behavior.put("has_search", !searchDates.isEmpty());
            behavior.put("search_days", new ArrayList<>(searchDates.keySet()));
            behavior.put("has_purchase", !purchaseDates.isEmpty());
            behavior.put("purchase_days", new ArrayList<>(purchaseDates.keySet()));
            behavior.put("has_browse", !browseDates.isEmpty());
            behavior.put("browse_days", new ArrayList<>(browseDates.keySet()));
            json.put("behavior", behavior);

            // 时间段分布
            json.put("time_slots", analyzeTimeSlots());

            // 搜索关键词 TOP10
            json.put("top_search_keywords", new ArrayList<>(searchKeywords).subList(
                    0, Math.min(10, searchKeywords.size())));

            // 浏览商品数
            json.put("browsed_products_count", browsedProducts.size());

            // 订单数
            json.put("order_count", orderIds.size());

            // 设备信息
            if (deviceInfo != null) {
                json.put("device", deviceInfo.toJson());
            }

            // 网络类型
            json.put("network_types", networkTypes);

            // 用户标签
            json.put("user_tags", generateUserTags());

            return json.toJSONString();
        }

        /**
         * 转为每日汇总 JSON
         */
        public String toDailySummaryJson() {
            JSONObject json = new JSONObject(true);
            json.put("user_id", userId);
            json.put("profile_type", "daily_summary");
            json.put("summary_date", LocalDate.now().format(DATE_FORMATTER));
            json.put("total_login_days", loginDates.size());
            json.put("total_orders", orderIds.size());
            json.put("total_browsed_products", browsedProducts.size());
            json.put("total_search_keywords", searchKeywords.size());
            json.put("user_level", calculateUserLevel());
            return json.toJSONString();
        }

        /**
         * 分析时间段偏好
         */
        private JSONObject analyzeTimeSlots() {
            JSONObject result = new JSONObject(true);
            
            int morningCount = 0;   // 6-12
            int afternoonCount = 0; // 12-18
            int eveningCount = 0;   // 18-24
            int nightCount = 0;     // 0-6

            for (Map.Entry<Integer, Integer> entry : timeSlots.entrySet()) {
                int hour = entry.getKey();
                int count = entry.getValue();

                if (hour >= 6 && hour < 12) morningCount += count;
                else if (hour >= 12 && hour < 18) afternoonCount += count;
                else if (hour >= 18 && hour < 24) eveningCount += count;
                else nightCount += count;
            }

            result.put("morning_6_12", morningCount);
            result.put("afternoon_12_18", afternoonCount);
            result.put("evening_18_24", eveningCount);
            result.put("night_0_6", nightCount);

            // 主要活跃时段
            String primarySlot = "morning";
            int maxCount = morningCount;
            if (afternoonCount > maxCount) {
                primarySlot = "afternoon";
                maxCount = afternoonCount;
            }
            if (eveningCount > maxCount) {
                primarySlot = "evening";
                maxCount = eveningCount;
            }
            if (nightCount > maxCount) {
                primarySlot = "night";
            }

            result.put("primary_time_slot", primarySlot);
            return result;
        }

        /**
         * 生成用户标签
         */
        private List<String> generateUserTags() {
            List<String> tags = new ArrayList<>();

            // 活跃度标签
            if (loginDates.size() >= 7) {
                tags.add("高频用户");
            } else if (loginDates.size() >= 3) {
                tags.add("活跃用户");
            } else {
                tags.add("低频用户");
            }

            // 转化标签
            if (!purchaseDates.isEmpty()) {
                tags.add("已转化");
                if (orderIds.size() >= 3) {
                    tags.add("高价值用户");
                }
            } else {
                tags.add("潜在用户");
            }

            // 行为标签
            if (!searchDates.isEmpty()) {
                tags.add("有搜索意图");
            }
            if (browsedProducts.size() >= 10) {
                tags.add("高浏览用户");
            }

            // 时间段标签
            JSONObject slots = analyzeTimeSlots();
            tags.add(slots.getString("primary_time_slot") + "活跃");

            return tags;
        }

        /**
         * 计算用户等级
         */
        private String calculateUserLevel() {
            int score = 0;
            score += loginDates.size() * 2;
            score += orderIds.size() * 10;
            score += browsedProducts.size();
            score += searchKeywords.size();

            if (score >= 100) return "VIP";
            if (score >= 50) return "高级";
            if (score >= 20) return "中级";
            return "普通";
        }
    }

    /**
     * 设备信息
     */
    static class DeviceInfo implements java.io.Serializable {
        String brand;
        String model;
        String platform;
        String platformVersion;

        public DeviceInfo(String brand, String model, String platform, String platformVersion) {
            this.brand = brand;
            this.model = model;
            this.platform = platform;
            this.platformVersion = platformVersion;
        }

        public JSONObject toJson() {
            JSONObject json = new JSONObject();
            json.put("brand", brand);
            json.put("model", model);
            json.put("platform", platform);
            json.put("platform_version", platformVersion);
            return json;
        }
    }
}