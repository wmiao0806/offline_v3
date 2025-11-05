package work1105;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserPathAnalysis {
    public static void main(String[] args) throws Exception {

        // 1. 环境初始化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 历史日志数据（PostgreSQL JDBC 批读取）
        tableEnv.executeSql(
                "CREATE TABLE history_logs ( " +
                        "  id BIGINT, " +
                        "  log_id STRING, " +
                        "  log JSON, " +
                        "  ts TIMESTAMP(3) " +
                        ") WITH ( " +
                        "  'connector' = 'jdbc', " +
                        "  'url' = 'jdbc:postgresql://192.168.200.31:5432/spider_db', " +
                        "  'table-name' = 'logs_user_info_message', " +
                        "  'username' = 'postgres', " +
                        "  'password' = '123456' " +
                        ")"
        );

        // 3. 实时日志数据（PostgreSQL CDC）
        tableEnv.executeSql(
                "CREATE TABLE realtime_logs ( " +
                        "  id BIGINT, " +
                        "  log_id STRING, " +
                        "  log JSON, " +
                        "  ts TIMESTAMP(3), " +
                        "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND " +
                        ") WITH ( " +
                        "  'connector' = 'postgres-cdc', " +
                        "  'hostname' = 'localhost', " +
                        "  'port' = '5432', " +
                        "  'username' = 'postgres', " +
                        "  'password' = '123456', " +
                        "  'database-name' = 'spider_db', " +
                        "  'schema-name' = 'public', " +
                        "  'table-name' = 'logs_user_info_message', " +
                        "  'slot.name' = 'flink_cdc_slot', " +
                        "  'decoding.plugin.name' = 'pgoutput' " +
                        ")"
        );

        // 4. 合并历史 + 实时数据
        tableEnv.executeSql(
                "CREATE VIEW all_logs AS " +
                        "SELECT * FROM history_logs " +
                        "UNION ALL " +
                        "SELECT * FROM realtime_logs"
        );

        // 5. 解析日志字段，提取用户ID、页面、事件、时间戳
        tableEnv.executeSql(
                "CREATE VIEW parsed_logs AS " +
                        "SELECT " +
                        "  log_id, " +
                        "  log->>'user_id' AS user_id, " +
                        "  log->>'page' AS page, " +
                        "  log->>'event' AS event, " +
                        "  ts " +
                        "FROM all_logs"
        );

        // 6. 路径分析（按用户 + 时间排序，分析页面流向）
        tableEnv.executeSql(
                "CREATE VIEW path_sequence AS " +
                        "SELECT " +
                        "  user_id, " +
                        "  LAG(page, 1) OVER (PARTITION BY user_id ORDER BY ts) AS last_page, " +
                        "  page AS current_page, " +
                        "  ts " +
                        "FROM parsed_logs"
        );

        // 7. 输出路径变化到控制台
        tableEnv.executeSql(
                "CREATE TABLE print_sink ( " +
                        "  user_id STRING, " +
                        "  last_page STRING, " +
                        "  current_page STRING, " +
                        "  ts TIMESTAMP(3) " +
                        ") WITH ( 'connector' = 'print' )"
        );

        // 8. 写出结果
        tableEnv.executeSql(
                "INSERT INTO print_sink " +
                        "SELECT user_id, last_page, current_page, ts FROM path_sequence"
        );
    }
}
