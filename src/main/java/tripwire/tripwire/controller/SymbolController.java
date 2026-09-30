package tripwire.tripwire.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/symbols")
@RequiredArgsConstructor
public class SymbolController {

    private final RedisTemplate<String, Object> redisTemplate;
    private final RestClient restClient = RestClient.create();

    @Value("${app.stock.api.alpha_key:demo}")
    private String alphaKey;

    @PostMapping("/add")
    public ResponseEntity<?> addSymbol(@RequestParam String symbol, @RequestParam String market) {
        String upperSymbol = symbol.toUpperCase();
        
        // Basic Validation check using AlphaVantage
        try {
            String url = "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=" + upperSymbol + "&apikey=" + alphaKey;
            String response = restClient.get().uri(url).retrieve().body(String.class);
            if (response == null || response.contains("Error Message") || response.contains("Invalid API call")) {
                return ResponseEntity.badRequest().body(Map.of("message", "Symbol not found or invalid"));
            }
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("message", "Validation failed: " + e.getMessage()));
        }

        // Store in Redis based on market
        String redisKey = market.equalsIgnoreCase("CRYPTO") ? "symbols:crypto" : 
                         market.equalsIgnoreCase("INDIA") ? "symbols:india" : "symbols:us";
        
        redisTemplate.opsForSet().add(redisKey, upperSymbol);
        return ResponseEntity.ok(Map.of("message", "Symbol added successfully"));
    }

    @GetMapping("/list")
    public ResponseEntity<?> listSymbols() {
        return ResponseEntity.ok(Map.of(
            "us", redisTemplate.opsForSet().members("symbols:us"),
            "india", redisTemplate.opsForSet().members("symbols:india"),
            "crypto", redisTemplate.opsForSet().members("symbols:crypto")
        ));
    }
}