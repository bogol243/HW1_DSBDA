import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    /**
     * Test for single broken string
     */
    @Test
    public void testMapperCounterOne() throws IOException  {
        String brokenSring = "*****************";
        mapDriver
                .withInput(new LongWritable(), new Text(brokenSring))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    /**
     * Test for single valid string
     */
    @Test
    public void testMapperCounterZero() throws IOException {

        /** s1: Sample string from my laptop's syslog*/
        String s1 = "5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\"";        /** Expected key for s1*/
        String s1_key = "2021-03-20 00";
        /** Expected value for s1*/
        int s1_val = 5;

        mapDriver
                .withInput(new LongWritable(),new Text(s1))
                .withOutput(new Text(s1_key),new IntWritable(s1_val))
                .runTest();
        assertEquals("Expected 0 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    /**
     * Test for both broken and valid strings
     */
    @Test
    public void testMapperCounters() throws IOException {
        String s1 = "5: 2021-03-20 00:01:07 dm-pc kernel:[ 6300.903550] audit: type=1400 audit(1616187667.960:34481): apparmor=\"DENIED\" operation=\"ptrace\" profile=\"snap.discord.discord\" pid=14848 comm=\"Discord\" requested_mask=\"read\" denied_mask=\"read\" peer=\"unconfined\"";
        /** Expected key for s1*/
        String s1_key = "2021-03-20 00";
        /** Expected value for s1*/
        int s1_val = 5;
        String brokenString = "***********";
        mapDriver
                .withInput(new LongWritable(), new Text(brokenString))
                .withInput(new LongWritable(), new Text(s1))
                .withInput(new LongWritable(), new Text(brokenString))
                .withOutput(new Text(s1_key),new IntWritable(s1_val))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}