package com.scaleset.search;

import com.scaleset.utils.Extensible;

public class Stats extends Extensible {

    private Long count;
    private Double sum;
    private Double max;
    private Double min;
    private Double mean;
    private Double sumOfSquares;
    private Double variance;
    private Double stdDeviation;

    protected Stats() {
    }

    public Stats(long count, double sum, double min, double max, double mean, double sumOfSquares, double variance, double stdDeviation) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.sumOfSquares = sumOfSquares;
        this.variance = variance;
        this.stdDeviation = stdDeviation;
    }

    public Stats(long count, double sum, double min, double max, double mean) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.sumOfSquares = 0.0;
        this.variance = 0.0;
        this.stdDeviation = 0.0;
    }

    public long getCount() {
        return count;
    }

    public double getMax() {
        return max;
    }

    public double getMean() {
        return mean;
    }

    public double getMin() {
        return min;
    }

    public double getStdDeviation() {
        return stdDeviation;
    }

    public double getSumOfSquares() {
        return sumOfSquares;
    }

    public double getSum() {
        return sum;
    }

    public double getVariance() {
        return variance;
    }
}
