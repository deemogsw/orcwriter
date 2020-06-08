import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @ClassName orcWriteImpl2
 * @Description TODO
 * @Author 94928
 * @Date 2020/6/8 23:21
 * @Version 1.0
 **/
public class orcWriteImpl2 {
    public static void main(String[] args) throws IOException {
        String path = "D:\\Idea_workspace2\\mystudy\\orcwrite\\src\\main\\result";
        Path testFilePath = new Path(path,"advanced-example.orc");
        Configuration conf = new Configuration();

        TypeDescription schema =
                TypeDescription.fromString("struct<first:int," +
                        "second:int,third:map<string,int>>");

        Writer writer =
                OrcFile.createWriter(testFilePath,
                        OrcFile.writerOptions(conf).setSchema(schema));

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];
        LongColumnVector second = (LongColumnVector) batch.cols[1];

//Define map. You need also to cast the key and value vectors
        MapColumnVector map = (MapColumnVector) batch.cols[2];
        BytesColumnVector mapKey = (BytesColumnVector) map.keys;
        LongColumnVector mapValue = (LongColumnVector) map.values;

// Each map has 5 elements
        final int MAP_SIZE = 5;
        final int BATCH_SIZE = batch.getMaxSize();

// Ensure the map is big enough
        mapKey.ensureSize(BATCH_SIZE * MAP_SIZE, false);
        mapValue.ensureSize(BATCH_SIZE * MAP_SIZE, false);

// add 1500 rows to file
        for(int r=0; r < 1500; ++r) {
            int row = batch.size++;

            first.vector[row] = r;
            second.vector[row] = r * 3;

            map.offsets[row] = map.childCount;
            map.lengths[row] = MAP_SIZE;
            map.childCount += MAP_SIZE;

            for (int mapElem = (int) map.offsets[row];
                 mapElem < map.offsets[row] + MAP_SIZE; ++mapElem) {
                String key = "row " + r + "." + (mapElem - map.offsets[row]);
                mapKey.setVal(mapElem, key.getBytes(StandardCharsets.UTF_8));
                mapValue.vector[mapElem] = mapElem;
            }
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }
}
