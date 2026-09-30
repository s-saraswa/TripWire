package tripwire.tripwire.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PriceUpdate {
    @JsonProperty("type")
    private String type; // "STOCK" or "CRYPTO"

    @JsonProperty("market")
    private String market;

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("change")
    private BigDecimal change;

    @JsonProperty("changePercent")
    private BigDecimal changePercent;

    @JsonProperty("volume")
    private Object volume; // Long for stocks, BigDecimal for crypto

    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("source")
    private String source;

    @JsonProperty("confidence")
    private Double confidence;

    @JsonProperty("sourceCount")
    private Integer sourceCount;
}