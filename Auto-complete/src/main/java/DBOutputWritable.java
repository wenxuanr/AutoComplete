import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
// 接口，提供方法
// 重写

public class DBOutputWritable implements Writable, DBWritable
{
    private String starting_phrase;
    private String following_words;
    private int count;

    public DBOutputWritable(String starting_phrase, String following_words,int count)
    {
        this.starting_phrase = starting_phrase;
        this.following_words = following_words;
        this.count = count;
    }

    public void readFields(DataInput in) throws IOException {   }

    public void readFields(ResultSet arg0) throws SQLException
    {
        this.starting_phrase = arg0.getString(1);
        this.following_words = arg0.getString(2);
        count = arg0.getInt(3);
    }

    public void write(DataOutput out) throws IOException {    }

    // 把什么数据写到database中
    // 不用管背后的机制
    public void write(PreparedStatement arg0) throws SQLException
    {
        arg0.setString(1, starting_phrase);
        arg0.setString(2, following_words);
        arg0.setInt(3, count);
    }
}