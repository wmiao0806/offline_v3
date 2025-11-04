import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PageViewStats {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 注册 Postgres CDC Source
        tEnv.executeSql(
                "CREATE TABLE logs_user_info_message (" +
                        " id BIGINT," +
                        " log_id STRING," +
                        " log STRING," +
                        " ts TIMESTAMP(3)," +
                        " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        " 'connector' = 'postgres-cdc'," +
                        " 'hostname' = '192.168.200.31'," +
                        " 'port' = '5432'," +
                        " 'username' = 'postgres'," +
                        " 'password' = 'wm352111henhaoji'," +
                        " 'database-name' = 'spider_db'," +
                        " 'schema-name' = 'public'," +
                        " 'table-name' = 'logs_user_info_message'," +
                        " 'slot.name' = 'flink_slot'," +
                        " 'decoding.plugin.name' = 'pgoutput'" +
                        ")"
        );

        // 2. 解析 JSON 并提取 log_type（即页面类型）
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW parsed_log AS " +
                        "SELECT " +
                        "  JSON_VALUE(log, '$.log_type') AS page, " + // 👈 改这里
                        "  CAST(ts AS DATE) AS visit_date " +
                        "FROM logs_user_info_message"
        );

        // 3. 按天、页面分组统计访问量
        tEnv.executeSql(
                "SELECT " +
                        "  visit_date, " +
                        "  page, " +
                        "  COUNT(*) AS pv " +
                        "FROM parsed_log " +
                        "GROUP BY visit_date, page"
        ).print();

        env.execute("Page View Statistics");
    }
}
