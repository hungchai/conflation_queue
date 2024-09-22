package util;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static String getTimeStamp() {
        LocalDateTime now = LocalDateTime.now();
        String timestampWithoutNano = now.format(DATE_TIME_FORMATTER);
        long nanoTime = System.nanoTime() % 1_000_000_000L; // Reduce to nanoseconds in a second
        return timestampWithoutNano + String.format(".%09d", nanoTime);
    }

    public static void info(String message) {
        System.out.println(getTimeStamp() + " INFO: " + message);
    }

    public static void error(String message) {
        System.err.println(getTimeStamp() + " ERROR: " + message);
    }

    public static void error(String message, Exception ex) {
        System.err.println(getTimeStamp() + " ERROR: " + message);
        ex.printStackTrace(System.err);
    }

    public static void main(String[] args) {
        Logger.info("This is an info message.");
        Logger.error("This is an error message.");

        try {
            throw new RuntimeException("This is a test exception.");
        } catch (Exception ex) {
            Logger.error("An exception occurred", ex);
        }
    }
}
