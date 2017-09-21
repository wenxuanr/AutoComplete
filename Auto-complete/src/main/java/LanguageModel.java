import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.*;

public class LanguageModel {
    public static class ngramMapper
            extends Mapper<LongWritable, Text, Text, Text>{
        // input: is Ngram Library
        // input: I love big data\t10
        // output : key = I love big  value = data = 10
        // I love big -> data appears 10 times

        int threhold;
        @Override
        public void setup (Context context) {
            Configuration configuration = context.getConfiguration();
            threhold = configuration.getInt("threhold", 20);
        }

        @Override
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();

            String [] wordsPlusCount = line.split("\t");
            if (wordsPlusCount.length < 2) {
                return;
            }
            // split the last words as the value and remainning words as key;
            String [] words = wordsPlusCount[0].split("\\s+");
            // {I,love,big,data};  value is gone;
            int count = Integer.parseInt(wordsPlusCount[1]);

            if (count < threhold) {
                return;
            }
            // get output key and output value
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]);
                sb.append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
            // 数据写在hdfs上
            // will move to reducer later
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }
    public static class ngramReducer
            extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        // reduce method
        int topKey;
        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            topKey = configuration.getInt("topKey", 5);
        }
        @Override
        public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key = I love big
            // value <data = 10, girl = 100, boy = 1000.....>
            TreeMap<Integer,List<String>> tm;
            tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            // <10, <data,baby..>, <100, <girl>>>
            // word = 出现的频数
            for (Text val : values) {
                String value = val.toString();
                String word = value.split("=")[0].trim();
                int count = Integer.parseInt(value.split("=")[1].trim());
                if (tm.containsKey(count)) {
                    tm.get(count).add(word);
                }
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    tm.put(count,list);
                }
                // make top K selection
                Iterator<Integer> iter = tm.keySet().iterator();
                for (int j = 0; iter.hasNext() && j < topKey;) {
                    int keyCount = iter.next();
                    List<String> words = tm.get(keyCount);
                    for (int i = 0; i < words.size() && j < topKey; i++) {
                        context.write(new DBOutputWritable(key.toString(), words.get(i),keyCount),NullWritable.get());
                        j++;
                    }
                }

            }

        }
    }

}
