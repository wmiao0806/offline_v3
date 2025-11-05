package work1105;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SearchKeywordTop10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 注册 Postgres CDC 源表
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
                        " 'slot.name' = 'flink_search_slot'," +
                        " 'decoding.plugin.name' = 'pgoutput'" +
                        ")"
        );

        // 2. 提取搜索关键词（只保留 opa='search' 的日志）
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW search_logs AS " +
                        "SELECT " +
                        "  JSON_VALUE(log, '$.keyword') AS keyword, " +
                        "  JSON_VALUE(log, '$.opa') AS opa, " +
                        "  CAST(ts AS DATE) AS dt " +
                        "FROM logs_user_info_message " +
                        "WHERE JSON_VALUE(log, '$.opa') = 'search' " +
                        "  AND JSON_VALUE(log, '$.keyword') IS NOT NULL"
        );

        // 3. 按天、关键词聚合出现次数
        tEnv.executeSql(
                "CREATE TEMPORARY VIEW keyword_count AS " +
                        "SELECT dt, keyword, COUNT(*) AS cnt " +
                        "FROM search_logs " +
                        "GROUP BY dt, keyword"
        );

        // 4. 每天取 TOP10
        tEnv.executeSql(
                "SELECT dt, keyword, cnt " +
                        "FROM (" +
                        "  SELECT dt, keyword, cnt, " +
                        "         ROW_NUMBER() OVER (PARTITION BY dt ORDER BY cnt DESC) AS rn " +
                        "  FROM keyword_count" +
                        ") WHERE rn <= 10"
        ).print();

        env.execute("Daily Search Keyword Top10");
    }
}
