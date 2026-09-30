package tripwire.tripwire.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tripwire.tripwire.service.PriceQueryService;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/health")
@RequiredArgsConstructor
public class HealthController {

    private final PriceQueryService queryService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> health() {
        long stockCount = queryService.getAllStockPrices().size();
        long cryptoCount = queryService.getAllCryptoPrices().size();

        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "stocksTracked", stockCount,
                "cryptoTracked", cryptoCount,
                "timestamp", System.currentTimeMillis()
        ));
    }

    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> ready() {
        return ResponseEntity.ok(Map.of("status", "READY"));
    }
}