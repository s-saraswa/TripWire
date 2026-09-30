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
public class StockPrice {
    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("market")
    private String market;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("change")
    private BigDecimal change;

    @JsonProperty("changePercent")
    private BigDecimal changePercent;

    @JsonProperty("volume")
    private Long volume;

    @JsonProperty("high")
    private BigDecimal high;

    @JsonProperty("low")
    private BigDecimal low;

    @JsonProperty("open")
    private BigDecimal open;

    @JsonProperty("previousClose")
    private BigDecimal previousClose;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("source")
    private String source;
}