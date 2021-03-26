package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class: implements the map operation of Hadoop map-reduce pipeline
 *
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Overrided map function: processes line of syslog file to map date and time of event to event identifier.
     * Date and time(floored to hours) are represented by single {@link String}({@link Text} object.
     * @param key - the input key
     * @param value - input value. A single line from syslog file
     * @param context - object that allows the Mapper/Reducer to interact with the rest of the Hadoop system.
     * @see HW1Reducer - mapper of this application
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        ParsedLog log = parseLogString(line);
        if (log.event == -1) {
            //something went wrong
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            Text datetime = new Text();
            datetime.set(log.datetime);
            IntWritable eventId = new IntWritable(log.event);
            context.write(datetime, eventId);
        }
    }

    /**
     * A helper function that parses given line of syslog file to ParsedLog object
     * @param str - single line of syslog
     * @return ParsedLog object that contains datetime and eventId of system event
     * @see ParsedLog
     */
    public static ParsedLog parseLogString(String str){
        ParsedLog res = new ParsedLog();
        //super simple parser
        String[] strings = str.split(" ");
        //super simple check
        if(strings.length!=17){
            res.event = -1;
            return res;
        }
        String[] timeStrings = strings[2].split(":");
        res.datetime = strings[1]+" "+timeStrings[0];
        res.event = Integer.parseInt(strings[0].split(":")[0]);
        return res;
    }
}
