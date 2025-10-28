import com.stream.common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @Package com.stream.realtime.lululemon.FlkTestSQL
 * @Author zhou.han
 * @Date 2025/10/27 08:53
 * @description:
 */
public class FlkTestSQL {

    @SneakyThrows
    public static void main(String[] args) {

//        System.setProperty("HADOOP_USER_NAME","root");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
//        env.setParallelism(1);
////        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//        env.setStateBackend(new HashMapStateBackend());
//        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
////        tenv.getConfig().getConfiguration().setString("table.local-time-zone", "Asia/Shanghai");
//        tenv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "30 s");
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage());

//        // 然后覆盖 RocksDB 配置
//        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage());


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        tenv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        String source_kafka_order_info_ddl = "create table if not exists t_kafka_oms_order_info (\n" +
                "    id string,\n" +
                "    order_id string,\n" +
                "    user_id string,\n" +
                "    user_name string,\n" +
                "    phone_number string,\n" +
                "    product_link string,\n" +
                "    product_id string,\n" +
                "    color string,\n" +
                "    size string,\n" +
                "    item_id string,\n" +
                "    material string,\n" +
                "    sale_num string,\n" +
                "    sale_amount string,\n" +
                "    total_amount string,\n" +
                "    product_name string,\n" +
                "    is_online_sales string,\n" +
                "    shipping_address string,\n" +
                "    recommendations_product_ids string,\n" +
                "    ds string,\n" +
                "    ts bigint,\n" +
                "    ts_ms as case when ts < 100000000000 then to_timestamp_ltz(ts * 1000, 3) else to_timestamp_ltz(ts, 3) end,\n" +
                "    insert_time string,\n" +
                "    table_name string,\n" +
                "    op string,\n" +
                "    watermark for ts_ms as ts_ms - interval '5' second\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'realtime_v3_logs',\n" +
                "    'properties.bootstrap.servers'= '192.168.200.30:9092',\n" +
                "    'properties.group.id' = 'order-analysis1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        tenv.executeSql(source_kafka_order_info_ddl);
        tenv.executeSql("SELECT * from t_kafka_oms_order_info LIMIT 10").print();
//        tenv.executeSql("select count(1) as cnt from t_kafka_oms_order_info").print();


//        String windowAggSQL =
//                "CREATE TABLE v_window_sales_with_wm (\n" +
//                        "    window_start TIMESTAMP(3),\n" +
//                        "    window_end TIMESTAMP(3),\n" +
//                        "    rowtime AS CAST(window_end AS TIMESTAMP_LTZ(3)),\n" +
//                        "    sale_amount_sum DOUBLE,\n" +
//                        "    cert_time STRING,\n" +
//                        "    WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n" +
//                        ")\n" +
//                        "WITH (\n" +
//                        "    'connector' = 'print'\n" +  // 用 print sink 做演示
//                        ")";
//
//        tenv.executeSql(windowAggSQL);
//
//        String insertSQL =
//                "INSERT INTO v_window_sales_with_wm\n" +
//                        "SELECT \n" +
//                        "    window_start,\n" +
//                        "    window_end,\n" +
//                        "    SUM(CAST(sale_amount AS DOUBLE)) AS sale_amount_sum,\n" +
//                        "    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS cert_time\n" +
//                        "FROM TABLE(\n" +
//                        "    TUMBLE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' SECOND)\n" +
//                        ")\n" +
//                        "GROUP BY window_start, window_end";
//
//        tenv.executeSql(insertSQL);
////        tenv.executeSql("SELECT * FROM t_kafka_oms_order_info").print();
//
//        String aa = "SELECT window_start, window_end, SUM(CAST(sale_amount AS DOUBLE)) \n" +
//                "FROM TABLE(\n" +
//                "    TUMBLE(TABLE t_kafka_oms_order_info, DESCRIPTOR(ts_ms), INTERVAL '10' SECOND')\n" +
//                ")\n" +
//                "GROUP BY window_start, window_end;\n";
//        tenv.executeSql(aa).print();

        env.execute("LuLulemon Today Cumulative GMV Calculation");
    }

}
