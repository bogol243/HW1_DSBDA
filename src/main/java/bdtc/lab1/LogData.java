package bdtc.lab1;


import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogData {
    Map<String, Integer> myMap = new HashMap<String, Integer>() {{
        put("debug", 7);
        put("info", 6);
        put("notice", 5);
        put("warning", 4);
        put("error", 3);
        put("crit", 2);
        put("alert", 1);
        put("panic", 0);
    }};
}
