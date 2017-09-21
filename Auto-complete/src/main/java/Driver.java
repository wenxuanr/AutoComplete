import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import javax.xml.soap.Text;
import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        // input Dir
        // 写到hdfs上
        // ouput dir
        // Nogram

        String inputDir =  args[0];
        String outputDir = args[1];
        String Nogram = args[2];
        String threshold = args[3];
        String topK = args[4];

        // job1
        // all input from teminal will get to Configration
        Configuration conf1 = new Configuration();
        // makes mapper 一句句读，传到mapreduce job里面去
        // 需要一句一句的读
        conf1.set("textinputformat.record.delimiter",".");
        conf1.set("NoGram",Nogram);

        Job job1 = Job.getInstance();
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(NgramLibrary.ngramMapper.class);
        job1.setReducerClass(NgramLibrary.ngramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1,new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(outputDir));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("threhold", threshold);
        conf2.set("topK",topK);

        DBConfiguration.configureDB(conf2,"com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.59.135:50070","root","'");


        Job job2 = Job.getInstance();
        job2.setJobName("LanguageModel");
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(LanguageModel.ngramMapper.class);
        job2.setReducerClass(LanguageModel.ngramReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);


        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job2,new Path(inputDir));
        TextOutputFormat.setOutputPath(job2, new Path(outputDir));

        job2.waitForCompletion(true);
    }
}
