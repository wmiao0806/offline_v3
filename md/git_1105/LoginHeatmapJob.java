package work1105;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LoginHeatmapJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. PostgreSQL CDC Source
        tEnv.executeSql(
                "CREATE TABLE logs_user_info_message ( " +
                        "  id BIGINT, " +
                        "  log_id STRING, " +
                        "  log STRING, " +
                        "  ts TIMESTAMP(3), " +
                        "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND " +
                        ") WITH ( " +
                        "  'connector' = 'postgres-cdc', " +
                        "  'hostname' = '192.168.200.31', " +
                        "  'port' = '5432', " +
                        "  'username' = 'postgres', " +
                        "  'password' = 'wm352111henhaoji', " +
                        "  'database-name' = 'spider_db', " +
                        "  'schema-name' = 'public', " +
                        "  'table-name' = 'logs_user_info_message', " +
                        "  'decoding.plugin.name' = 'pgoutput', " +
                        "  'slot.name' = 'flink_search_slot' " +
                        ")"
        );

        // 2. 创建视图提取省份
        tEnv.executeSql(
                "CREATE VIEW login_region AS " +
                        "SELECT " +
                        "  JSON_VALUE(log, '$.province') AS province, " +
                        "  JSON_VALUE(log, '$.city') AS city, " +
                        "  ts " +
                        "FROM logs_user_info_message " +
                        "WHERE JSON_VALUE(log, '$.action') = 'login'"
        );

        // 3. 统计访问次数（可带时间窗口）
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW region_heatmap AS " +
                        "SELECT " +
                        "  province, " +
                        "  COUNT(*) AS visit_count " +
                        "FROM login_region " +
                        "GROUP BY province"
        );

        // 4. 输出到控制台
        tEnv.executeSql(
                "CREATE TABLE print_sink (" +
                        "  province STRING, " +
                        "  visit_count BIGINT " +
                        ") WITH ('connector' = 'print')"
        );

        // 5. 将聚合结果写出
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM region_heatmap");
    }
}
