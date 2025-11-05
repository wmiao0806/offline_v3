package work1105;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DeviceStatsJob {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 1. 注册 Postgres 表
        tableEnv.executeSql(
            "CREATE TABLE user_device_base (\n" +
            "    id INT,\n" +
            "    brand STRING,\n" +
            "    plat STRING,\n" +
            "    platv STRING,\n" +
            "    softv STRING,\n" +
            "    uname STRING,\n" +
            "    userkey STRING,\n" +
            "    datatype STRING,\n" +
            "    device STRING,\n" +
            "    ip STRING,\n" +
            "    net STRING,\n" +
            "    opa STRING,\n" +
            "    ts TIMESTAMP(3),\n" +
            "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
            ") WITH (\n" +
            "    'connector' = 'jdbc',\n" +
            "    'url' = 'jdbc:postgresql://192.168.200.31:5432/spider_db',\n" +
            "    'table-name' = 'user_device_base',\n" +
            "    'username' = 'postgres',\n" +
            "    'password' = 'wm352111henhaoji'\n" +
            ")"
        );

        // 2. 注册控制台输出表（print connector）
        tableEnv.executeSql(
            "CREATE TABLE print_sink (\n" +
            "    brand STRING,\n" +
            "    plat STRING,\n" +
            "    net STRING,\n" +
            "    user_cnt BIGINT,\n" +
            "    data_type STRING\n" +
            ") WITH ('connector'='print')"
        );

        // 3. 执行 SQL
        tableEnv.executeSql(
            "INSERT INTO print_sink \n" +
            "SELECT * FROM (\n" +
            "   SELECT brand, plat, net, COUNT(DISTINCT userkey) AS user_cnt, 'history' AS data_type\n" +
            "   FROM user_device_base\n" +
            "   WHERE CAST(ts AS DATE) < CURRENT_DATE\n" +
            "   GROUP BY brand, plat, net\n" +
            "   UNION ALL\n" +
            "   SELECT brand, plat, net, COUNT(DISTINCT userkey) AS user_cnt, 'today' AS data_type\n" +
            "   FROM user_device_base\n" +
            "   WHERE CAST(ts AS DATE) = CURRENT_DATE\n" +
            "   GROUP BY brand, plat, net\n" +
            ")"
        );
    }
}
