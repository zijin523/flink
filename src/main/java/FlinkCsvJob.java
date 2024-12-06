import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.function.SerializableFunction;

import java.util.function.Function;

public class FlinkCsvJob {
    public static class ComplexPojo {
        private long id;
        private int[] array;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // CSV 文件路径
        String inputPath = "src/main/java/data.csv";


        // 启用 checkpointing，每 5 秒执行一次
        env.enableCheckpointing(5000);
        // 配置 checkpoint 容错
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 设置检查点超时
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); // 最大容错失败检查点次数

        SerializableFunction<CsvMapper, CsvSchema> schemaGenerator = new SerializableFunction<CsvMapper, CsvSchema>() {
            @Override
            public CsvSchema apply(CsvMapper mapper) {
                return mapper.schemaFor(CityPojo.class)
                        .withoutQuoteChar()  // No quotes around fields
                        .withColumnSeparator('|');  // Custom column separator
            }
        };

        CsvReaderFormat<CityPojo> csvFormat = CsvReaderFormat.forSchema(
                CsvMapper::new,
                schemaGenerator,
                TypeInformation.of(CityPojo.class)
        );

        // 使用 FileSource 读取
        FileSource<CityPojo> source = FileSource
                .forRecordStreamFormat(csvFormat, new Path(inputPath))
                .build();

        // 生成 DataStream
        DataStreamSource<CityPojo> dataStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "CSV Source"
        );

        dataStream.print();

        env.execute("Flink CSV Advanced Job");
    }
}



