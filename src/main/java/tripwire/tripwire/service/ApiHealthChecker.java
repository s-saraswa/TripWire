package tripwire.tripwire.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class ApiHealthChecker implements CommandLineRunner {

    private final RestClient restClient = RestClient.create();
    private final Map<String, Boolean> apiStatus = new ConcurrentHashMap<>();

    @Value("${app.stock.api.alpha_key:demo}")
    private String alphaKey;

    @Value("${app.stock.api.finnhub_key:demo}")
    private String finnhubKey;

    @Value("${app.stock.api.polygon_key:demo}")
    private String polygonKey;

    @Value("${app.crypto.api.cmc_key:demo}")
    private String cmcKey;

    @Override
    public void run(String... args) {
        log.info("Performing startup health check for APIs...");

        checkApi("ALPHA_VANTAGE", "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=" + alphaKey);
        checkApi("FINNHUB", "https://finnhub.io/api/v1/quote?symbol=AAPL&token=" + finnhubKey);
        checkApi("POLYGON", "https://api.polygon.io/v2/last/trade/AAPL?apiKey=" + polygonKey);
        checkApi("CMC", "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=BTC"); // Note: CMC needs header, handled in actual service
    }

    private void checkApi(String name, String url) {
        try {
            // For CMC, we just check if the endpoint is reachable; key is checked during actual runtime
            String response = restClient.get().uri(url).retrieve().body(String.class);
            if (response != null && !response.contains("Error") && !response.contains("Invalid API key")) {
                apiStatus.put(name, true);
                log.info("✅ API {} is Healthy", name);
            } else {
                apiStatus.put(name, false);
                log.warn("❌ API {} returned an error response", name);
            }
        } catch (Exception e) {
            apiStatus.put(name, false);
            log.error("❌ API {} is Unreachable: {}", name, e.getMessage());
        }
    }

    public boolean isApiAlive(String name) {
        return apiStatus.getOrDefault(name, false);
    }
}