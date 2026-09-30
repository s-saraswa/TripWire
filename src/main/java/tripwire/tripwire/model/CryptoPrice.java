package tripwire.tripwire.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CryptoPrice {
    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("change24h")
    private BigDecimal change24h;

    @JsonProperty("changePercent24h")
    private BigDecimal changePercent24h;

    @JsonProperty("volume24h")
    private BigDecimal volume24h;

    @JsonProperty("high24h")
    private BigDecimal high24h;

    @JsonProperty("low24h")
    private BigDecimal low24h;

    @JsonProperty("marketCap")
    private BigDecimal marketCap;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("source")
    private String source;
}