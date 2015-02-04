package parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AvroParquetInputFormatTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Job job;

    private Schema original = new Schema.Parser().parse("{\"namespace\": \"parquet.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Account\",\n" +
            " \"fields\": [\n" +
            "   {\"name\": \"id\", \"type\": \"long\"} ,\n" +
            "   {\"name\": \"person_id\", \"type\": \"long\"}]}");

    private Schema latest = new Schema.Parser().parse("{\"namespace\": \"parquet.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Account\",\n" +
            " \"fields\": [\n" +
            "   {\"name\": \"id\", \"type\": \"long\"} ,\n" +
            "   {\"name\": \"person_id\", \"type\": [\"null\",\"long\"], \"default\": null}]} ");
    private File origOutput;
    private File latestOutput;

    @Before
    public void whenGettingSplits() throws IOException {
        final Configuration conf = new Configuration();
        job = Job.getInstance(conf);
        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.getConfiguration().set(ParquetInputFormat.READ_SUPPORT_CLASS, AvroReadSupport.class.getName());
        AvroParquetInputFormat.setAvroReadSchema(job, latest);

        origOutput = folder.newFolder("orig");
        latestOutput = folder.newFolder("latest");

        AvroParquetWriter<GenericRecord> origWriter = new AvroParquetWriter<GenericRecord>(new Path(new File(origOutput, "out.parquet").toURI()), original, CompressionCodecName.UNCOMPRESSED, 1000, 1000, false);
        AvroParquetWriter<GenericRecord> latestWriter = new AvroParquetWriter<GenericRecord>(new Path(new File(latestOutput, "out.parquet").toURI()), latest, CompressionCodecName.UNCOMPRESSED, 1000, 1000, false);

        GenericRecordBuilder orig = new GenericRecordBuilder(original);
        orig.set("id", 1L);
        orig.set("person_id", 10L);
        origWriter.write(orig.build());
        origWriter.close();

        GenericRecordBuilder latest = new GenericRecordBuilder(this.latest);
        latest.set("id", 2L);
        latestWriter.write(latest.build());
        latestWriter.close();
    }

    @Test
    public void shouldGetSplitsWhenMergingWithReadSchemaFromOldToNew() throws Exception {
        FileInputFormat.addInputPath(job, new Path(origOutput.toURI()));
        FileInputFormat.addInputPath(job, new Path(latestOutput.toURI()));
        AvroParquetInputFormat inputFormat = new AvroParquetInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertThat(splits.size(), is(2));
    }

    @Test
    public void shouldGetSplitsWhenMergingWithReadSchemaFromNewToOld() throws Exception {
        FileInputFormat.addInputPath(job, new Path(latestOutput.toURI()));
        FileInputFormat.addInputPath(job, new Path(origOutput.toURI()));
        AvroParquetInputFormat inputFormat = new AvroParquetInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertThat(splits.size(), is(2));
    }
}
