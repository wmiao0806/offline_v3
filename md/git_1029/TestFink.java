package com.stream.realtime.lululemon;


import com.stream.common.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import java.time.ZoneId;

public class TestFink {

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用检查点
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Flink_HDFS/flink-checkpoints");
        env.setParallelism(1);

        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new HashMapStateBackend());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        // 1. 创建Kafka源表
        String source_kafka_order_info_ddl = "create table if not exists realtime_v3_oms_order_dtl (\n" +
                "    after ROW<\n" +
                "        id string,\n" +
                "        order_id string,\n" +
                "        user_id string,\n" +
                "        user_name string,\n" +
                "        phone_number string,\n" +
                "        product_link string,\n" +
                "        product_id string,\n" +
                "        color string,\n" +
                "        size string,\n" +
                "        item_id string,\n" +
                "        material string,\n" +
                "        sale_num string,\n" +
                "        sale_amount string,\n" +
                "        total_amount string,\n" +
                "        product_name string,\n" +
                "        is_online_sales string,\n" +
                "        shipping_address string,\n" +
                "        recommendations_product_ids string,\n" +
                "        ds string,\n" +
                "        ts bigint,\n" +
                "        insert_time bigint\n" +
                "    >,\n" +
                "    source ROW<\n" +
                "        version string,\n" +
                "        connector string,\n" +
                "        name string,\n" +
                "        ts_ms bigint,\n" +
                "        snapshot string,\n" +
                "        db string,\n" +
                "        schema string,\n" +
                "        `table` string,\n" +
                "        commit_lsn string\n" +
                "    >,\n" +
                "    op string,\n" +
                "    ts_ms bigint,\n" +
                "    event_time AS CASE \n" +
                "        WHEN after.ts < 100000000000 THEN TO_TIMESTAMP_LTZ(after.ts * 1000, 3) \n" +
                "        ELSE TO_TIMESTAMP_LTZ(after.ts, 3) \n" +
                "    END,\n" +
                "    watermark for event_time as event_time - interval '5' second\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_oms_order_dtl222',\n" +
                "    'properties.bootstrap.servers'= '172.27.252.79:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tEnv.executeSql(source_kafka_order_info_ddl);



//        tEnv.executeSql(
//                "SELECT\n" +
//                        "  window_start,\n" +
//                        "  window_end,\n" +
//                        "  SUM(CAST(after.total_amount AS DECIMAL(18,2))) AS gmv \n" +
//                        "FROM TABLE(\n" +
//                        "  CUMULATE(\n" +
//                        "    TABLE realtime_v3_oms_order_dtl,\n" +
//                        "    DESCRIPTOR(event_time),\n" +
//                        "    INTERVAL '10' MINUTE,\n" +
//                        "    INTERVAL '1' DAY\n" +
//                        "  )\n" +
//                        ")\n" +
//                        "WHERE event_time >= FLOOR(CURRENT_TIMESTAMP TO DAY)\n" +
//                        "GROUP BY window_start, window_end;"
//        ).print();


//        tEnv.executeSql(
//                "SELECT\n" +
//                        "  window_start,\n" +
//                        "  window_end,\n" +
//                        "  SUM(CAST(after.total_amount AS DECIMAL(18,2))) AS gmv,\n" +
//                        "  LISTAGG(after.id, ',') AS order_ids \n" +
//                        "FROM TABLE(\n" +
//                        "  CUMULATE(\n" +
//                        "    TABLE realtime_v3_oms_order_dtl,\n" +
//                        "    DESCRIPTOR(event_time),\n" +
//                        "    INTERVAL '10' MINUTE,\n" +
//                        "    INTERVAL '1' DAY\n" +
//                        "  )\n" +
//                        ")\n" +
//                        "WHERE event_time >= FLOOR(CURRENT_TIMESTAMP TO DAY)\n" +
//                        "GROUP BY window_start, window_end;"
//        ).print();

// id
        tEnv.executeSql(
                "WITH product_sales AS (\n" +
                        "  SELECT\n" +
                        "    window_start,\n" +
                        "    window_end,\n" +
                        "    product_id,\n" +
                        "    SUM(CAST(sale_num AS BIGINT)) AS total_sales\n" +
                        "  FROM TABLE(\n" +
                        "    CUMULATE(\n" +
                        "      TABLE realtime_v3_oms_order_dtl,\n" +
                        "      DESCRIPTOR(event_time),\n" +
                        "      INTERVAL '10' MINUTE,\n" +
                        "      INTERVAL '1' DAY\n" +
                        "    )\n" +
                        "  )\n" +
                        "  WHERE event_time >= FLOOR(CURRENT_TIMESTAMP TO DAY)\n" +
                        "  GROUP BY window_start, window_end, product_id\n" +
                        "),\n" +
                        "ranked_products AS (\n" +
                        "  SELECT\n" +
                        "    window_start,\n" +
                        "    window_end,\n" +
                        "    product_id,\n" +
                        "    total_sales,\n" +
                        "    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total_sales DESC) as sales_rank\n" +
                        "  FROM product_sales\n" +
                        "),\n" +
                        "top5_products AS (\n" +
                        "  SELECT\n" +
                        "    window_start,\n" +
                        "    window_end,\n" +
                        "    LISTAGG(product_id || '(' || CAST(total_sales AS STRING) || ')', ', ') as top5_product_ids\n" +
                        "  FROM ranked_products\n" +
                        "  WHERE sales_rank <= 5\n" +
                        "  GROUP BY window_start, window_end\n" +
                        ")\n" +
                        "SELECT\n" +
                        "  o.window_start,\n" +
                        "  o.window_end,\n" +
                        "  SUM(CAST(o.total_amount AS DECIMAL(18,2))) AS gmv,\n" +
                        "  LISTAGG(o.id, ',') AS order_ids,\n" +
                        "  t.top5_product_ids\n" +
                        "FROM TABLE(\n" +
                        "  CUMULATE(\n" +
                        "    TABLE realtime_v3_oms_order_dtl,\n" +
                        "    DESCRIPTOR(event_time),\n" +
                        "    INTERVAL '10' MINUTE,\n" +
                        "    INTERVAL '1' DAY\n" +
                        "  )\n" +
                        ") o\n" +
                        "LEFT JOIN top5_products t ON o.window_start = t.window_start AND o.window_end = t.window_end\n" +
                        "WHERE o.event_time >= FLOOR(CURRENT_TIMESTAMP TO DAY)\n" +
                        "GROUP BY o.window_start, o.window_end, t.top5_product_ids;"
        ).print();


//        // doris
//        String dorisSinkDDL = "CREATE TABLE IF NOT EXISTS order_window_summary (\n" +
//                "    window_start TIMESTAMP(3),\n" +
//                "    window_end TIMESTAMP(3),\n" +
//                "    total_sum DECIMAL(10,2)\n" +

//                ") WITH (\n" +
//                "    'connector' = 'doris',\n" +
//                "    'fenodes' = '192.168.200.32:8030',\n" +
//                "    'table.identifier' = 'spider_db.order_window_summary',\n" +
//                "    'username' = 'root',\n" +
//                "    'password' = '',\n" +
//                "    'sink.buffer-flush.max-rows' = '1000',\n" +
//                "    'sink.buffer-flush.interval' = '5000ms',\n" +
//                "    'sink.max-retries' = '3',\n" +
//                "    'sink.properties.format' = 'json',\n" +
//                "    'sink.properties.read_json_by_line' = 'true'\n" +
//                ")";

//        tEnv.executeSql(dorisSinkDDL);


        env.execute("TestFink");

    }
}
