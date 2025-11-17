package work1116;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

// 假设你已经有 HBaseUtil 工具类写入 HBase
public class KafkaAsyncJoinToHBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 1. 创建 Kafka Source
        KafkaSource<String> postgresSource = KafkaSource.<String>builder()
                .setBootstrapServers("your_kafka_bootstrap")
                .setTopics("postgres_1117")
                .setGroupId("group_postgres")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        KafkaSource<String> sqlServerSource = KafkaSource.<String>builder()
                .setBootstrapServers("your_kafka_bootstrap")
                .setTopics("sqlserver_1117")
                .setGroupId("group_sqlserver")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        DataStream<String> postgresStream = env.fromSource(postgresSource, WatermarkStrategy.noWatermarks(), "Postgres Source");
        DataStream<String> sqlServerStream = env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "SQLServer Source");

        // 2. 异步查询 HBase 或 Redis 做关联
        DataStream<String> joinedStream = AsyncDataStream.unorderedWait(
                postgresStream,
                new AsyncFunction<String, String>() {
                    @Override
                    public void asyncInvoke(String postgresRecord, ResultFuture<String> resultFuture) throws Exception {
                        // 假设用 sqlServerStream 做 lookup（简化示例）
                        // 或者查询 HBase/Redis 获取匹配数据
                        String result = postgresRecord + "_joined"; // 这里处理你的关联逻辑
                        resultFuture.complete(Collections.singleton(result));
                    }
                },
                5000, // timeout 5s
                TimeUnit.MILLISECONDS,
                20 // 最大并发数
        );

        // 3. 写入 HBase
        joinedStream.addSink(new HBaseSinkFunction());

        env.execute("Kafka Async Join to HBase");
    }
}
