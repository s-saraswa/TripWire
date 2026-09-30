package tripwire.tripwire.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import tripwire.tripwire.model.CryptoPrice;
import tripwire.tripwire.model.StockPrice;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PriceQueryService {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String STOCK_PRICE_KEY = "stock:price:";
    private static final String CRYPTO_PRICE_KEY = "crypto:price:";
    private static final String STOCK_HISTORY_KEY = "stock:history:";
    private static final String CRYPTO_HISTORY_KEY = "crypto:history:";

    @SuppressWarnings("unchecked")
    public StockPrice getLatestStockPrice(String symbol) {
        String key = STOCK_PRICE_KEY + symbol.toUpperCase();
        Object value = redisTemplate.opsForValue().get(key);
        return value instanceof StockPrice ? (StockPrice) value : null;
    }

    @SuppressWarnings("unchecked")
    public CryptoPrice getLatestCryptoPrice(String symbol) {
        String key = CRYPTO_PRICE_KEY + symbol.toLowerCase();
        Object value = redisTemplate.opsForValue().get(key);
        return value instanceof CryptoPrice ? (CryptoPrice) value : null;
    }

    public List<StockPrice> getAllStockPrices() {
        Set<String> keys = redisTemplate.keys(STOCK_PRICE_KEY + "*");
        if (keys == null || keys.isEmpty()) {
            return List.of();
        }
        return redisTemplate.opsForValue().multiGet(keys).stream()
                .filter(v -> v instanceof StockPrice)
                .map(v -> (StockPrice) v)
                .collect(Collectors.toList());
    }

    public List<CryptoPrice> getAllCryptoPrices() {
        Set<String> keys = redisTemplate.keys(CRYPTO_PRICE_KEY + "*");
        if (keys == null || keys.isEmpty()) {
            return List.of();
        }
        return redisTemplate.opsForValue().multiGet(keys).stream()
                .filter(v -> v instanceof CryptoPrice)
                .map(v -> (CryptoPrice) v)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public List<StockPrice> getStockHistory(String symbol, Instant from, Instant to, int limit) {
        String key = STOCK_HISTORY_KEY + symbol.toUpperCase();
        Set<Object> range = redisTemplate.opsForZSet().rangeByScore(
                key, from.toEpochMilli(), to.toEpochMilli(), 0, limit);
        if (range == null) {
            return List.of();
        }
        return range.stream()
                .filter(v -> v instanceof StockPrice)
                .map(v -> (StockPrice) v)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public List<CryptoPrice> getCryptoHistory(String symbol, Instant from, Instant to, int limit) {
        String key = CRYPTO_HISTORY_KEY + symbol.toLowerCase();
        Set<Object> range = redisTemplate.opsForZSet().rangeByScore(
                key, from.toEpochMilli(), to.toEpochMilli(), 0, limit);
        if (range == null) {
            return List.of();
        }
        return range.stream()
                .filter(v -> v instanceof CryptoPrice)
                .map(v -> (CryptoPrice) v)
                .collect(Collectors.toList());
    }
}