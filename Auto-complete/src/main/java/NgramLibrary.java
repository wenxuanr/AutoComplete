import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
// p of (word/phrase)  =  Count (phrase + word) / Count (Phrase)
// configiration like system property in Java
// mapper 讲单词进行划分（进行多次划分）
// reducer 每两个单词或者每3个单词中出现的次数
public class NgramLibrary {
    public static class ngramMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{
        // map method
        int noGram;
        @Override
        public void setup (Context context) {
            Configuration conf = context.getConfiguration();
            noGram =  conf.getInt("noGram" , 5);
        }
        @Override
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input : read sentence
            // I love big,
            // if n = 3
            // divide
            // I love, 1
            // love big, 1
            // context makes mapper/reducer interact with rest of the hadoop system
            /**
            ***
            ***Configuration conf = context.getConfiguration();
            ***int noGram = conf.getInt("NoGram",5);
            ***
            ***/
            // if ngram is null , apply 5 to it
            // 非 a-z的 replace成空格
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]]"," ");
            // split with space
            // maybe a lot of space
            String []words = line.split("\\s+");
            // if < 2, no need to spilit anymore
            if (words.length < 2) {
                return;
            }
            StringBuilder sb;
            for (int i = 0; i < words.length; i++) {
                sb = new StringBuilder();
                // append current word
                sb.append(words[i]);
                for (int j = 1; i + j < words.length && j < noGram; j++) {
                    sb.append(" ");
                    // add current word
                    sb.append(words[i+j]);
                     // 每个value肯定都是一,reducer will merge later;
                    context.write(new Text(sb.toString()),new IntWritable(1));
                }
            }



        }
    }
    public static class ngramReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce method
        @Override
        public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();//Indeed, value is always 1;
            }
            // merge
            context.write(key, new IntWritable(sum));
        }
    }

}
