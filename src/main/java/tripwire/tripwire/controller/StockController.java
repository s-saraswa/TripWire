package tripwire.tripwire.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tripwire.tripwire.model.CryptoPrice;
import tripwire.tripwire.model.StockPrice;
import tripwire.tripwire.service.PriceQueryService;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/stocks")
@RequiredArgsConstructor
public class StockController {

    private final PriceQueryService queryService;

    @GetMapping("/{symbol}")
    public ResponseEntity<StockPrice> getStockPrice(@PathVariable String symbol) {
        StockPrice price = queryService.getLatestStockPrice(symbol);
        return price != null ? ResponseEntity.ok(price) : ResponseEntity.notFound().build();
    }

    @GetMapping
    public ResponseEntity<List<StockPrice>> getAllStockPrices() {
        return ResponseEntity.ok(queryService.getAllStockPrices());
    }

    @GetMapping("/{symbol}/history")
    public ResponseEntity<List<StockPrice>> getStockHistory(
            @PathVariable String symbol,
            @RequestParam(required = false) Instant from,
            @RequestParam(required = false) Instant to,
            @RequestParam(defaultValue = "100") int limit) {
        Instant fromTime = from != null ? from : Instant.now().minusSeconds(3600);
        Instant toTime = to != null ? to : Instant.now();
        return ResponseEntity.ok(queryService.getStockHistory(symbol, fromTime, toTime, limit));
    }

    @GetMapping("/symbols")
    public ResponseEntity<List<String>> getTrackedSymbols() {
        List<String> symbols = queryService.getAllStockPrices().stream()
                .map(StockPrice::getSymbol)
                .sorted()
                .toList();
        return ResponseEntity.ok(symbols);
    }
}