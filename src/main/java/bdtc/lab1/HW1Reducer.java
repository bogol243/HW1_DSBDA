package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, Text> {

    /** Dictionary that maps event ids to their string representations*/
    static Map<Integer, String> num_to_name = new HashMap<Integer, String>() {{
        put(7,"debug");
        put(6,"info");
        put(5,"notice");
        put(4,"warning");
        put(3,"error");
        put(2,"critical");
        put(1,"alert");
        put(0,"panic");
    }};

    /**
     * reduce function: for each datetime({@link Text}) value it counts the number of happened events.
     * Then it produces the output string that contains the names of events and number of it's occurrences.
     * @param key - date and time(floored to hours) in represented by {@link String} object
     * @param values - an {@link Iterable} object that contains the {@link IntWritable} value, which are an event ids
     *               of events hat happened in time interval defined by key.
     * @param context - object that allows the Mapper/Reducer to interact with the rest of the Hadoop system.
     * @see HW1Mapper - mapper of this application
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int[] sum = new int[8];
        for(int i =0;i<8;i++){
            sum[i]=0;
        }

        while (values.iterator().hasNext()) {
            sum[values.iterator().next().get()]++;
        }

        String res = "";
        boolean first = true;
        for(int i=0;i<8;i++){
            if(!first && sum[i]!=0){
                res+=", ";
            }else if(sum[i]!=0){
                first=false;
            }
            if(sum[i]==0) continue;
            res += num_to_name.get(i)+": "+Integer.toString(sum[i]);
        }

        context.write(key, new Text(res));
    }
}
