import bdtc.lab1.ParsedLog;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Class for testing map stage, reduce stage, and both stages together.
 */
public class MapReduceTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    /**
     * Setting up the drivers needed to perform tests
     */
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    /**
     * Test to verify functionality of map stage of application
     */
    @Test
    public void testMapper() throws IOException {
        /** s1: Sample string from my laptop's syslog*/
        String s1 = "5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\"";
        /** Expected key for s1*/
        String s1_key = "2021-03-20 00";
        /** Expected value for s1*/
        int s1_val = 5;
        mapDriver
                .withInput(new LongWritable(),new Text(s1))
                .withOutput(new Text(s1_key),new IntWritable(s1_val))
                .runTest();
    }

    /**
     * Test to verify functionality of reduce stage of application
     */
    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("11-11-11 11"), values)
                .withOutput(new Text("11-11-11 11"), new Text("alert: 2"))
                .runTest();
    }


    /**
     * Test to verify the functionality of both stages of mapreduce application.
     */
    @Test
    public void testMapReduce() throws IOException{
        String input[] = {
                "5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\""
                ,"5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\""
                ,"5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\""
                ,"6: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\""
                ,"6: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\""
                ,"6: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\""};
        mapReduceDriver
                .withInput(new LongWritable(), new Text(input[0]))
                .withInput(new LongWritable(), new Text(input[1]))
                .withInput(new LongWritable(), new Text(input[2]))
                .withInput(new LongWritable(), new Text(input[3]))
                .withInput(new LongWritable(), new Text(input[4]))
                .withInput(new LongWritable(), new Text(input[5]))
                .withOutput(new Text("2021-03-20 00"),new Text("notice: 3, info: 3"))
                .runTest();
    }

    /**
     *  Test to verify the functionality of HW1Mapper.parseLogString() function
     * @see HW1Mapper
     */
    @Test
    public void testParser() throws IOException{
        String s1 = "5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\"";
        ParsedLog pl = HW1Mapper.parseLogString(s1);
        ParsedLog expected = new ParsedLog();
        expected.event = 5;
        expected.datetime = "2021-03-20 00";
        assertEquals("parsed log is wrong",pl.datetime,expected.datetime);
    }
}