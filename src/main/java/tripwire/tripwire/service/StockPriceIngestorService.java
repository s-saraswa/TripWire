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
import tripwire.tripwire.model.StockPrice;

@Slf4j
@Service
@RequiredArgsConstructor
public class StockPriceIngestorService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PriceAggregatorService aggregator;
    private final ApiHealthChecker healthChecker;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    private final RestClient restClient = RestClient.create();

    @Value("${app.stock.symbols:AAPL,GOOGL,MSFT,AMZN,TSLA}")
    private String symbolsConfig;

    @Value("${app.stock.api.alpha_key:demo}")
    private String alphaKey;

    @Value("${app.stock.api.finnhub_key:demo}")
    private String finnhubKey;

    @Value("${app.stock.api.polygon_key:demo}")
    private String polygonKey;

    private List<String> getSymbols() {
        return Arrays.stream(symbolsConfig.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    @Scheduled(fixedDelayString = "${app.stock.fetch.interval:30000}")
    public void fetchAndPublishStockPrices() {
        List<String> symbols = getSymbolsFromRedis("symbols:us");
        if (symbols.isEmpty()) {
            log.debug("No US stocks to track");
            return;
        }
        log.info("Aggregating prices for {} US symbols", symbols.size());

        for (String symbol : symbols) {
            try {
                List<CompletableFuture<BigDecimal>> futures = new java.util.ArrayList<>();
                
                if (healthChecker.isApiAlive("ALPHA_VANTAGE")) {
                    futures.add(CompletableFuture.supplyAsync(() -> fetchAlphaVantage(symbol)));
                }
                if (healthChecker.isApiAlive("FINNHUB")) {
                    futures.add(CompletableFuture.supplyAsync(() -> fetchFinnhub(symbol)));
                }
                if (healthChecker.isApiAlive("POLYGON")) {
                    futures.add(CompletableFuture.supplyAsync(() -> fetchPolygon(symbol)));
                }

                if (futures.isEmpty()) {
                    log.warn("No healthy APIs available to fetch stock prices");
                    continue;
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

                List<BigDecimal> prices = futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList());

                BigDecimal avgPrice = aggregator.calculateAverage(prices);

                if (avgPrice.compareTo(BigDecimal.ZERO) > 0) {
                    StockPrice aggregated = new StockPrice();
                    aggregated.setSymbol(symbol);
                    aggregated.setMarket("US");
                    aggregated.setPrice(avgPrice);
                    aggregated.setTimestamp(Instant.now());
                    aggregated.setSource("Aggregated (" + prices.size() + " sources)");
                    
                    kafkaTemplate.send(KafkaTopicConfig.STOCK_PRICES_TOPIC, symbol, objectMapper.writeValueAsString(aggregated));
                }
            } catch (Exception e) {
                log.error("Error aggregating {}: {}", symbol, e.getMessage());
            }
        }
    }

    private List<String> getSymbolsFromRedis(String key) {
        Set<Object> members = redisTemplate.opsForSet().members(key);
        return members == null ? List.of() : members.stream().map(Object::toString).toList();
    }

    @org.springframework.context.event.EventListener(org.springframework.context.event.ContextRefreshedEvent.class)
    public void initSymbols() {
        String[] defaults = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"};
        redisTemplate.opsForSet().add("symbols:us", (Object[]) defaults);
    }

    private BigDecimal fetchAlphaVantage(String symbol) {
        try {
            String url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + symbol + "&apikey=" + alphaKey;
            JsonNode root = objectMapper.readTree(restClient.get().uri(url).retrieve().body(String.class));
            return new BigDecimal(root.path("Global Quote").path("05. price").asText("0"));
        } catch (Exception e) { return null; }
    }

    private BigDecimal fetchFinnhub(String symbol) {
        try {
            String url = "https://finnhub.io/api/v1/quote?symbol=" + symbol + "&token=" + finnhubKey;
            JsonNode root = objectMapper.readTree(restClient.get().uri(url).retrieve().body(String.class));
            return new BigDecimal(root.path("c").asText("0"));
        } catch (Exception e) { return null; }
    }

    private BigDecimal fetchPolygon(String symbol) {
        try {
            String url = "https://api.polygon.io/v2/last/trade/" + symbol + "?apiKey=" + polygonKey;
            JsonNode root = objectMapper.readTree(restClient.get().uri(url).retrieve().body(String.class));
            return new BigDecimal(root.path("results").path("p").asText("0"));
        } catch (Exception e) { return null; }
    }
}