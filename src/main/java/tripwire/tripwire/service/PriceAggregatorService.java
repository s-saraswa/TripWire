package tripwire.tripwire.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

@Service
public class PriceAggregatorService {

    public List<BigDecimal> filterOutliers(List<BigDecimal> prices) {
        if (prices == null || prices.isEmpty()) return Collections.emptyList();

        // Remove nulls first
        List<BigDecimal> filtered = prices.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (filtered.size() <= 1) return filtered;

        // Outlier removal: while the difference between max and min is > 5%, remove the lower one
        while (filtered.size() > 1) {
            BigDecimal max = Collections.max(filtered);
            BigDecimal min = Collections.min(filtered);
            
            // Calculate % difference: (Max - Min) / Max
            BigDecimal diff = max.subtract(min);
            BigDecimal percentDiff = diff.divide(max, 4, RoundingMode.HALF_UP);

            if (percentDiff.compareTo(new BigDecimal("0.05")) > 0) {
                filtered.remove(min);
            } else {
                break; // Difference is within 5%
            }
        }
        return filtered;
    }

    public BigDecimal calculateAverage(List<BigDecimal> prices) {
        List<BigDecimal> filtered = filterOutliers(prices);

        if (filtered.isEmpty()) return BigDecimal.ZERO;

        BigDecimal sum = filtered.stream()
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        return sum.divide(BigDecimal.valueOf(filtered.size()), 4, RoundingMode.HALF_UP);
    }

    public Double calculateConfidence(List<BigDecimal> prices, BigDecimal average) {
        List<BigDecimal> filtered = filterOutliers(prices);

        if (filtered.isEmpty()) return 0.0;
        if (filtered.size() == 1) return 0.5; // Low confidence for single source

        double totalDeviation = 0;
        for (BigDecimal price : filtered) {
            double diff = price.subtract(average).abs().doubleValue();
            totalDeviation += diff / average.doubleValue();
        }

        double avgDeviation = totalDeviation / filtered.size();
        return Math.max(0.0, 1.0 - avgDeviation);
    }
}