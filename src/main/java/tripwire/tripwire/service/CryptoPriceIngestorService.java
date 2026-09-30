package tripwire.tripwire.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.data.redis.core.RedisTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import tripwire.tripwire.config.KafkaTopicConfig;
import tripwire.tripwire.model.CryptoPrice;

@Slf4j
@Service
@RequiredArgsConstructor
public class CryptoPriceIngestorService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PriceAggregatorService aggregator;
    private final ApiHealthChecker healthChecker;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    private final RestClient restClient = RestClient.create();

    @Value("${app.crypto.symbols:bitcoin,ethereum,solana}")
    private String symbolsConfig;

    @Value("${app.crypto.api.cmc_key:demo}")
    private String cmcKey;

    @Scheduled(fixedDelayString = "${app.crypto.fetch.interval:30000}")
    public void fetchAndPublishCryptoPrices() {
        List<String> symbols = getSymbolsFromRedis();
        if (symbols.isEmpty()) {
            log.debug("No crypto symbols to track");
            return;
        }
        log.info("Aggregating crypto prices for {} symbols", symbols.size());

        for (String symbol : symbols) {
            try {
                List<CompletableFuture<BigDecimal>> futures = new java.util.ArrayList<>();
                futures.add(CompletableFuture.supplyAsync(() -> fetchCoinGecko(symbol)));
                futures.add(CompletableFuture.supplyAsync(() -> fetchBinance(symbol)));
                if (healthChecker.isApiAlive("CMC")) {
                    futures.add(CompletableFuture.supplyAsync(() -> fetchCMC(symbol)));
                }

                if (futures.isEmpty()) continue;

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                List<BigDecimal> prices = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
                BigDecimal avgPrice = aggregator.calculateAverage(prices);

                if (avgPrice.compareTo(BigDecimal.ZERO) > 0) {
                    CryptoPrice aggregated = new CryptoPrice();
                    aggregated.setSymbol(symbol.toUpperCase());
                    aggregated.setPrice(avgPrice);
                    aggregated.setTimestamp(Instant.now());
                    aggregated.setSource("Aggregated (" + prices.size() + " sources)");
                    kafkaTemplate.send(KafkaTopicConfig.CRYPTO_PRICES_TOPIC, symbol, objectMapper.writeValueAsString(aggregated));
                }
            } catch (Exception e) {
                log.error("Error aggregating crypto {}: {}", symbol, e.getMessage());
            }
        }
    }

    private List<String> getSymbolsFromRedis() {
        Set<Object> members = redisTemplate.opsForSet().members("symbols:crypto");
        return members == null ? List.of() : members.stream().map(Object::toString).toList();
    }

    @org.springframework.context.event.EventListener(org.springframework.context.event.ContextRefreshedEvent.class)
    public void initSymbols() {
        String[] defaults = {"bitcoin", "ethereum", "solana"};
        redisTemplate.opsForSet().add("symbols:crypto", (Object[]) defaults);
    }

    private BigDecimal fetchCoinGecko(String symbol) {
        try {
            String url = "https://api.coingecko.com/api/v3/simple/price?ids=" + symbol + "&vs_currencies=usd";
            JsonNode root = objectMapper.readTree(restClient.get().uri(url).retrieve().body(String.class));
            return new BigDecimal(root.path(symbol).path("usd").asText("0"));
        } catch (Exception e) { return null; }
    }

    private BigDecimal fetchBinance(String symbol) {
        try {
            String pair = symbol.toUpperCase().replace("bitcoin", "BTC").replace("ethereum", "ETH").replace("solana", "SOL") + "USDT";
            String url = "https://api.binance.com/api/v3/ticker/price?symbol=" + pair;
            JsonNode root = objectMapper.readTree(restClient.get().uri(url).retrieve().body(String.class));
            return new BigDecimal(root.path("price").asText("0"));
        } catch (Exception e) { return null; }
    }

    private BigDecimal fetchCMC(String symbol) {
        try {
            String url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=" + symbol.toUpperCase();
            String response = restClient.get()
                    .uri(url)
                    .header("X-CMC_PRO_API_KEY", cmcKey)
                    .retrieve()
                    .body(String.class);
            JsonNode parsedRoot = objectMapper.readTree(response);
            return new BigDecimal(parsedRoot.path("data").path(symbol.toUpperCase()).path("quote").path("USD").path("price").asText("0"));
        } catch (Exception e) { 
            log.error("CMC API Error for {}: {}", symbol, e.getMessage());
            return null; 
        }
    }
}