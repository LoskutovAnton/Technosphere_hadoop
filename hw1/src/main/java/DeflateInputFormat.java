import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflateInputFormat extends FileInputFormat<LongWritable, Text> {

    public class PageRecordReader extends RecordReader<LongWritable, Text> {
        private FSDataInputStream data_file;
        private FSDataInputStream idx_file;

        private short idx_size = 4;
        private byte[] idx_buf = new byte[idx_size];
        private int page_size = 4096;
        private byte[] page_buf = new byte[page_size];

        private long nrecords = 0;
        private long cur_record = 0;
        private long key = 0;
        private Text text = new Text("");

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            FileSplit file_split = (FileSplit) split;

            Path idx_file_path = file_split.getPath();

            Path path = new Path(idx_file_path.toString().replaceAll(".idx", ""));
            FileSystem fs = path.getFileSystem(conf);
            FileSystem idx_fs = idx_file_path.getFileSystem(conf);

            this.nrecords = file_split.getLength() / idx_size;

            data_file = fs.open(path);
            idx_file = idx_fs.open(idx_file_path);

            int shift_file = 0;

            for (int shift_idx_file = 0; shift_idx_file < file_split.getStart(); shift_idx_file += 4) {
                idx_file.readFully(idx_buf, 0, idx_buf.length);
                shift_file += ByteBuffer.wrap(idx_buf).order(ByteOrder.LITTLE_ENDIAN).getInt();
            }

            key = shift_file;
            data_file.seek(shift_file);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (cur_record >= nrecords) {
                return false;
            }
            Inflater inflater = new Inflater();

            idx_file.readFully(idx_buf, 0, idx_buf.length);
            byte[] cur_page = new byte[ByteBuffer.wrap(idx_buf).order(ByteOrder.LITTLE_ENDIAN).getInt()];
            data_file.readFully(cur_page, 0, cur_page.length);

            inflater.setInput(cur_page);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            for (int len = 0; !inflater.finished(); bos.write(page_buf, 0, len), len = 0) {
                try {
                    len = inflater.inflate(page_buf, 0, page_buf.length);
                } catch (DataFormatException e) {
                    throw new IOException("Inflater ERROR: " + e.getMessage());
                }
            }
            bos.close();

            text = new Text(bos.toString(StandardCharsets.UTF_8.name()));
            cur_record++;
            key += cur_page.length;

            return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(this.key);
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return this.text;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return ((float) this.cur_record + 1) / this.nrecords;
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeStream(data_file);
            IOUtils.closeStream(idx_file);
        }
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        PageRecordReader reader = new PageRecordReader();
        reader.initialize(split, context);

        return reader;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<>();

        for (FileStatus status: listStatus(job)) {
            Path path = new Path(status.getPath().toString() + ".idx");
            FileSystem idx_fs = path.getFileSystem(job.getConfiguration());
            long idx_file_size = idx_fs.getFileStatus(path).getLen();
            long split_size = job.getConfiguration().getLong("mapreduce.input.indexedgz.bytespermap", 220000);

            for (long total = 0; total < idx_file_size; total += split_size) {
                if (total + split_size > idx_file_size) {
                    split_size = idx_file_size - total;
                }
                splits.add(new FileSplit(path, total, split_size, null));
            }
        }

        return splits;
    }
}