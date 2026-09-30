package tripwire.tripwire.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tripwire.tripwire.model.CryptoPrice;
import tripwire.tripwire.service.PriceQueryService;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/crypto")
@RequiredArgsConstructor
public class CryptoController {

    private final PriceQueryService queryService;

    @GetMapping("/{symbol}")
    public ResponseEntity<CryptoPrice> getCryptoPrice(@PathVariable String symbol) {
        CryptoPrice price = queryService.getLatestCryptoPrice(symbol);
        return price != null ? ResponseEntity.ok(price) : ResponseEntity.notFound().build();
    }

    @GetMapping
    public ResponseEntity<List<CryptoPrice>> getAllCryptoPrices() {
        return ResponseEntity.ok(queryService.getAllCryptoPrices());
    }

    @GetMapping("/{symbol}/history")
    public ResponseEntity<List<CryptoPrice>> getCryptoHistory(
            @PathVariable String symbol,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "100") int limit) {
        Instant fromTime = from != null ? from : Instant.now().minusSeconds(3600);
        Instant toTime = to != null ? to : Instant.now();
        return ResponseEntity.ok(queryService.getCryptoHistory(symbol, fromTime, toTime, limit));
    }

    @GetMapping("/symbols")
    public ResponseEntity<List<String>> getTrackedSymbols() {
        List<String> symbols = queryService.getAllCryptoPrices().stream()
                .map(CryptoPrice::getSymbol)
                .sorted()
                .toList();
        return ResponseEntity.ok(symbols);
    }
}