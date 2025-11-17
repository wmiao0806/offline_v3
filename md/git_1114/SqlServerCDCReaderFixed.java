package work1113;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class SqlServerCDCReaderFixed {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.KafkaAsyncJoinToHBase
        DbusSyncSqlserverOmsSysData2Kafka
        DbusSyncPostgreSqlOmsSysData2KafkagetExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        Properties debeziumProperties = new Properties();

        // ========== 关键配置：多种方式确保禁用 SSL ==========

        // 方法1：连接字符串（最可靠）
        debeziumProperties.setProperty(
                "database.connection.url",
                "jdbc:sqlserver://192.168.200.31:1433;" +
                        "databaseName=realtime_v3;" +
                        "encrypt=false;" +
                        "trustServerCertificate=true;" +
                        "loginTimeout=30"
        );

        // 方法2：独立属性（双重保险）
        debeziumProperties.setProperty("database.encrypt", "false");
        debeziumProperties.setProperty("database.trustServerCertificate", "true");

        // 方法3：Debezium 特定属性
        debeziumProperties.setProperty("database.ssl.mode", "disabled");
        debeziumProperties.setProperty("database.sslmode", "disable");

        // ========== 其他必要配置 ==========
        debeziumProperties.setProperty("decimal.handling.mode", "string");
        debeziumProperties.setProperty("bigint.unsigned.handling.mode", "long");
        debeziumProperties.setProperty("database.connectionTimeZone", "UTC");
        debeziumProperties.setProperty("snapshot.mode", "initial");

        // 连接和超时配置
        debeziumProperties.setProperty("connect.timeout.ms", "30000");
        debeziumProperties.setProperty("connection.pool.size", "1");
        debeziumProperties.setProperty("max.retries", "3");
        debeziumProperties.setProperty("retry.delay.ms", "1000");

        // 创建 SQL Server CDC Source
        SourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("192.168.200.31")
                .port(1433)
                .database("realtime_v3")
                .tableList("dbo.product_comment_details")
                .username("SA")
                .password("Wm352111henhaoji!")
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStream = env.addSource(sqlServerSource);
        dataStream.print();

        env.execute("Flink SQL Server CDC Job - Final Fix");
    }
}