package tomma.hft.conflatingqueue;

import org.HdrHistogram.Histogram;

public class PerformanceAnalyzer {
    private final Histogram histogram;

    public PerformanceAnalyzer() {
        // Min=1 microsecond, Max=1 second, and accuracy up to 3 decimal places
        this.histogram = new Histogram(1, 1_000_000_000L, 3);
    }

    public void recordExecutionTime(long executionTime) {
        histogram.recordValue(executionTime);
    }

    public void printHistogram() {
        System.out.println("Execution Time Histogram:");
        histogram.outputPercentileDistribution(System.out, 1.0);
    }
}