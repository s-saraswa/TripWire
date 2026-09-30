package tripwire.tripwire.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import tripwire.tripwire.config.KafkaTopicConfig;
import tripwire.tripwire.model.CryptoPrice;
import tripwire.tripwire.model.PriceUpdate;
import tripwire.tripwire.model.StockPrice;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class PriceProcessorService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    private static final String STOCK_PRICE_KEY = "stock:price:";
    private static final String CRYPTO_PRICE_KEY = "crypto:price:";
    private static final String STOCK_HISTORY_KEY = "stock:history:";
    private static final String CRYPTO_HISTORY_KEY = "crypto:history:";
    private static final int HISTORY_MAX_SIZE = 1000;
    private static final int TTL_HOURS = 24;

    @KafkaListener(topics = KafkaTopicConfig.STOCK_PRICES_TOPIC, groupId = "price-processor-group")
    public void processStockPrice(String message) {
        try {
            StockPrice price = objectMapper.readValue(message, StockPrice.class);
            price.setMarket("US");
            processAndBroadcast(price, STOCK_PRICE_KEY, STOCK_HISTORY_KEY, "STOCK");
        } catch (Exception e) {
            log.error("Error processing stock price: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopicConfig.INDIAN_STOCK_PRICES_TOPIC, groupId = "price-processor-group")
    public void processIndianStockPrice(String message) {
        try {
            StockPrice price = objectMapper.readValue(message, StockPrice.class);
            price.setMarket("INDIA");
            processAndBroadcast(price, STOCK_PRICE_KEY, STOCK_HISTORY_KEY, "STOCK");
        } catch (Exception e) {
            log.error("Error processing Indian stock price: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaTopicConfig.CRYPTO_PRICES_TOPIC, groupId = "price-processor-group")
    public void processCryptoPrice(String message) {
        try {
            CryptoPrice price = objectMapper.readValue(message, CryptoPrice.class);
            processAndBroadcast(price, CRYPTO_PRICE_KEY, CRYPTO_HISTORY_KEY, "CRYPTO");
        } catch (Exception e) {
            log.error("Error processing crypto price: {}", e.getMessage());
        }
    }

    private void processAndBroadcast(Object priceObj, String priceKeyPrefix, String historyKeyPrefix, String type) {
        String symbol;
        String market;
        BigDecimal price;
        BigDecimal change;
        BigDecimal changePercent;
        Object volume;
        Instant timestamp;
        String source;

        if (priceObj instanceof StockPrice stockPrice) {
            symbol = stockPrice.getSymbol();
            market = stockPrice.getMarket();
            price = stockPrice.getPrice();
            change = stockPrice.getChange();
            changePercent = stockPrice.getChangePercent();
            volume = stockPrice.getVolume();
            timestamp = stockPrice.getTimestamp();
            source = stockPrice.getSource();
        } else if (priceObj instanceof CryptoPrice cryptoPrice) {
            symbol = cryptoPrice.getSymbol();
            market = "CRYPTO";
            price = cryptoPrice.getPrice();
            change = cryptoPrice.getChange24h();
            changePercent = cryptoPrice.getChangePercent24h();
            volume = cryptoPrice.getVolume24h();
            timestamp = cryptoPrice.getTimestamp();
            source = cryptoPrice.getSource();
        } else {
            return;
        }

        // Store latest price in Redis
        String priceKey = priceKeyPrefix + symbol;
        if (priceObj instanceof StockPrice) {
            redisTemplate.opsForValue().set(priceKey, priceObj, TTL_HOURS, TimeUnit.HOURS);
        } else {
            redisTemplate.opsForValue().set(priceKey, priceObj, TTL_HOURS, TimeUnit.HOURS);
        }

        // Add to history (sorted set with timestamp as score)
        String historyKey = historyKeyPrefix + symbol;
        String priceJson;
        try {
            priceJson = objectMapper.writeValueAsString(priceObj);
            redisTemplate.opsForZSet().add(historyKey, priceJson, timestamp.toEpochMilli());
            // Trim history to max size
            Long size = redisTemplate.opsForZSet().size(historyKey);
            if (size != null && size > HISTORY_MAX_SIZE) {
                redisTemplate.opsForZSet().removeRange(historyKey, 0, size - HISTORY_MAX_SIZE - 1);
            }
            redisTemplate.expire(historyKey, TTL_HOURS, TimeUnit.HOURS);
        } catch (Exception e) {
            log.error("Error saving to history: {}", e.getMessage());
        }

        // Create unified update for WebSocket broadcast
        PriceUpdate update = new PriceUpdate();
        update.setType(type);
        update.setMarket(market);
        update.setSymbol(symbol);
        update.setPrice(price);
        update.setChange(change);
        update.setChangePercent(changePercent);
        update.setVolume(volume);
        update.setTimestamp(timestamp);
        update.setSource(source);
        update.setConfidence(1.0); // Default confidence if not provided by aggregator
        update.setSourceCount(1);

        // Broadcast to WebSocket subscribers
        try {
            String updateJson = objectMapper.writeValueAsString(update);
            messagingTemplate.convertAndSend("/topic/prices/" + type.toLowerCase() + "/" + symbol.toLowerCase(), updateJson);
            messagingTemplate.convertAndSend("/topic/prices/all", updateJson);
        } catch (Exception e) {
            log.error("Error broadcasting update: {}", e.getMessage());
        }

        log.debug("Processed and broadcast {} price for {}: {}", type, symbol, price);
    }
}