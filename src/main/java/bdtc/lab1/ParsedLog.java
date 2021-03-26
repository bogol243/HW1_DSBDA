package bdtc.lab1;

/**
 * A data structure that represent line of syslog file by it's datetime and event id
 * @author Dmitry Bogoslovsky
 * @version 2.0
 */
public class ParsedLog implements Comparable{
    /** field that stores the datetime of event*/
    public String datetime; //up to hour
    /** field that stored the event identifier*/
    public int event;

    /**
     * A function that compares ParsedLog object to other ParsedLog object.
     * Internally it uses the inbuilt comparators of Integer and String.
     * @param o - other {@link ParsedLog} object
     * @return an integer value that represent order before two ParsedLog objects.
     *
     */
    @Override
    public int compareTo(Object o) {
        ParsedLog other = (ParsedLog) o;
        if(other.datetime==datetime){
            return (new Integer(other.event)).compareTo(event);
        }else{
            return other.datetime.compareTo(datetime);
        }
    }
}
