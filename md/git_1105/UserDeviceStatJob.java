package work1105;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UserDeviceStatJob {
    public static void main(String[] args) throws Exception {

        // 环境初始化
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册 PostgreSQL CDC Source 表（历史 + 当天）
        tableEnv.executeSql(
                "CREATE TABLE user_device_base (" +
                        " id INT," +
                        " brand STRING," +
                        " plat STRING," +
                        " platv STRING," +
                        " softv STRING," +
                        " uname STRING," +
                        " userkey STRING," +
                        " datatype STRING," +
                        " device STRING," +
                        " ip STRING," +
                        " net STRING," +
                        " opa STRING," +
                        " ts TIMESTAMP(3)," +
                        " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        " 'connector' = 'postgres-cdc'," +
                        " 'hostname' = '192.168.200.31'," +        // 改成你的 Postgres 地址
                        " 'port' = '5432'," +
                        " 'username' = 'postgres'," +
                        " 'password' = 'wm352111henhaoji'," +    // 改成你的密码
                        " 'database-name' = 'spider_db'," +
                        " 'schema-name' = 'public'," +
                        " 'table-name' = 'user_device_base'," +
                        " 'slot.name' = 'flink_slot'," +
                        " 'decoding.plugin.name' = 'pgoutput'," +
                        " 'debezium.slot.name' = 'flink_slot'," +
                        " 'scan.startup.mode' = 'initial'" +
                        ")"
        );


        // 注册控制台 Sink
        tableEnv.executeSql(
                "CREATE TABLE print_sink (" +
                        " plat STRING," +
                        " brand STRING," +
                        " platv STRING," +
                        " day_str STRING," +
                        " cnt BIGINT" +
                        ") WITH (" +
                        " 'connector' = 'print'" +
                        ")"
        );

        // 写 SQL 查询逻辑
        String sql = "INSERT INTO print_sink " +
                "SELECT " +
                "  plat, " +
                "  brand, " +
                "  platv, " +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd') AS day_str, " +
                "  COUNT(DISTINCT userkey) AS cnt " +
                "FROM user_device_base " +
                "WHERE plat IN ('ios', 'android') " +
                "GROUP BY plat, brand, platv, DATE_FORMAT(ts, 'yyyy-MM-dd')";

        tableEnv.executeSql(sql);

        env.execute("User Device Distribution Job");
    }
}
