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
import tripwire.tripwire.config.KafkaTopicConfig;
import tripwire.tripwire.model.StockPrice;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class IndianStockIngestorService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
    private final RestClient restClient = RestClient.create();

    @Value("${app.indian.stock.symbols:RELIANCE.BSE,TCS.BSE,HDFCBANK.BSE,ICICIBANK.BSE,INFY.BSE,HINDUNILVR.BSE,SBIN.BSE,BHARTIARTL.BSE,ITC.BSE,LTIM.BSE}")
    private String symbolsConfig;

    @Value("${app.stock.api.key:demo}")
    private String apiKey;

    @Value("${app.stock.api.url:https://www.alphavantage.co/query}")
    private String apiUrl;

    private List<String> getSymbols() {
        return Arrays.stream(symbolsConfig.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @Scheduled(fixedDelayString = "${app.indian.stock.fetch.interval:60000}")
    public void fetchAndPublishIndianStockPrices() {
        List<String> symbols = getSymbols();
        log.info("Fetching Indian stock prices for {} symbols", symbols.size());

        for (String symbol : symbols) {
            try {
                StockPrice price = fetchStockPrice(symbol);
                if (price != null) {
                    String json = objectMapper.writeValueAsString(price);
                    kafkaTemplate.send(KafkaTopicConfig.INDIAN_STOCK_PRICES_TOPIC, symbol, json);
                    log.debug("Published Indian stock price for {}: {}", symbol, price.getPrice());
                }
            } catch (Exception e) {
                log.error("Error fetching Indian price for {}: {}", symbol, e.getMessage());
            }
        }
    }

    private StockPrice fetchStockPrice(String symbol) {
        try {
            String url = apiUrl + "?function=GLOBAL_QUOTE&symbol=" + symbol + "&apikey=" + apiKey;
            String response = restClient.get()
                    .uri(url)
                    .retrieve()
                    .body(String.class);

            JsonNode root = objectMapper.readTree(response);
            JsonNode quote = root.path("Global Quote");

            if (quote.isMissingNode() || quote.isEmpty()) {
                log.warn("No data for symbol: {}", symbol);
                return null;
            }

            BigDecimal price = new BigDecimal(quote.path("05. price").asText("0"));
            BigDecimal change = new BigDecimal(quote.path("09. change").asText("0"));
            BigDecimal changePercent = new BigDecimal(
                    quote.path("10. change percent").asText("0%").replace("%", ""));
            Long volume = quote.path("06. volume").asLong(0L);
            BigDecimal high = new BigDecimal(quote.path("03. high").asText("0"));
            BigDecimal low = new BigDecimal(quote.path("04. low").asText("0"));
            BigDecimal open = new BigDecimal(quote.path("02. open").asText("0"));
            BigDecimal previousClose = new BigDecimal(quote.path("08. previous close").asText("0"));

            return new StockPrice(
                    symbol, "INDIA", price, change, changePercent, volume,
                    high, low, open, previousClose,
                    Instant.now(), "AlphaVantage"
            );
        } catch (Exception e) {
            log.error("Failed to parse response for {}: {}", symbol, e.getMessage());
            return null;
        }
    }
}