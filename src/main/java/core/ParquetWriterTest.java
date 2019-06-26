package core;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * Created by HUANGHAI on 2019/6/21.
 */
public class ParquetWriterTest {
    private static String schemaStr = "message schema {" + "optional binary name;"
            + "optional int32 age;}";
    static MessageType schema = MessageTypeParser.parseMessageType(schemaStr);


    public static void main(String[] args) throws IOException {
        testParquetWriter();

    }

    private static void testParquetWriter() throws IOException {
        Path file = new Path(
                "E:\\idea_workspace\\spark-study-scala\\data\\localParquet\\output.parquet");
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(file).withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(schema);
        /*
         * file, new GroupWriteSupport(), CompressionCodecName.SNAPPY, 256 *
		 * 1024 * 1024, 1 * 1024 * 1024, 512, true, false,
		 * ParquetProperties.WriterVersion.PARQUET_1_0, conf
		 */
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        String[] access_log = {"Toy", "16"};
        for (int i = 0; i < 1000; i++) {
            writer.write(groupFactory.newGroup()
                    .append("name", access_log[0])
                    .append("age", Integer.parseInt(access_log[1])));
        }
        writer.close();
    }

}
