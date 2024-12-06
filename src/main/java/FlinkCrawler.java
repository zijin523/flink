import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class FlinkCrawler {
    public static void main(String[] args) throws Exception {
        // 初始化 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用自定义的 WebCrawlerSource 获取数据流
        DataStream<Tuple2<String, String>> webDataStream = env.addSource(new WebCrawlerSource());

        // 对抓取的数据进行简单处理，例如打印抓取的内容
        DataStream<String> result = webDataStream.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> value) throws Exception {
                return "Crawled URL: " + value.f0 + ", Content: " + value.f1;
            }
        });

        // 创建 Kafka
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");  // Kafka broker addresses
        kafkaProps.put("acks", "all");                          // Ensure data is fully acknowledged
        kafkaProps.put("retries", 3);                            // Set retries in case of failures
        kafkaProps.put("linger.ms", 5);                          // Time to wait before sending messages
        kafkaProps.put("batch.size", 16384);

        // 创建 Kafka Producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "flink-web-scraped-data",             // Kafka topic
                new SimpleStringSchema(),            // 序列化 schema
                kafkaProps                           // Kafka 配置
        );

        // 数据写入 Kafka
        result.addSink(kafkaProducer);

        result.print();

        env.execute("Flink Web Crawler Job");
    }
}

