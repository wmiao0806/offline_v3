package work1116;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import java.util.Properties;

public class DbusSyncPostgreSqlOmsSysData2Kafka {

    private static final String OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC = "postgres_1117";

    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 开启 checkpoint，保证 Exactly-Once
        env.enableCheckpointing(3000L); // 每3秒一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Debezium 配置
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        debeziumProperties.put("slot.name", "flink_cdc_slot");
        debeziumProperties.put("publication.name", "flink_cdc_publication");
        debeziumProperties.put("decoding.plugin.name", "pgoutput");

        // PostgreSQLSource
        DebeziumSourceFunction<String> postgresSource = PostgreSQLSource.<String>builder()
                .hostname("192.168.200.31")
                .port(5432)
                .username("postgres")
                .password("wm352111henhaoji")
                .database("spider_db")
                .schemaList("public")
                .tableList("public.user_info")
                .decodingPluginName("pgoutput")
                .slotName("flink_cdc_slot")
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(postgresSource, "_transaction_log_source1");

        // 将 JSON 字符串转换为 JSONObject
        SingleOutputStreamOperator<JSONObject> convertStr2JsonDs = dataStreamSource
                .map(JSON::parseObject)
                .uid("convertStr2JsonDs")
                .name("convertStr2JsonDs");

        // 使用 MapMergeJsonData 处理
        SingleOutputStreamOperator<JSONObject> mergedJsonDs = convertStr2JsonDs
                .map(new MapMergeJsonData())
                .uid("mapMergeJsonData")
                .name("mapMergeJsonData");

        // 控制台打印
        mergedJsonDs.print();

        // 写入 Kafka
        KafkaSink<JSONObject> kafkaSink = KafkaSink.<JSONObject>builder()
                .setBootstrapServers("192.168.200.30:9092") // 修改为你实际的 Kafka broker
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC)
                                .setValueSerializationSchema(new SerializationSchema<JSONObject>() {
                                    @Override
                                    public byte[] serialize(JSONObject element) {
                                        return element.toJSONString().getBytes();
                                    }
                                })
                                .build()
                )
                .build();

        mergedJsonDs.sinkTo(kafkaSink);

        env.execute("DbusSyncPostgreSqlOmsSysData2Kafka");
    }
}
