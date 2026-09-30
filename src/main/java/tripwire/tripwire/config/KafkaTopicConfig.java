package tripwire.tripwire.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String STOCK_PRICES_TOPIC = "stock-prices";
    public static final String INDIAN_STOCK_PRICES_TOPIC = "indian-stock-prices";
    public static final String CRYPTO_PRICES_TOPIC = "crypto-prices";
    public static final String PRICE_UPDATES_TOPIC = "price-updates";

    @Bean
    public NewTopic stockPricesTopic() {
        return TopicBuilder.name(STOCK_PRICES_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic indianStockPricesTopic() {
        return TopicBuilder.name(INDIAN_STOCK_PRICES_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cryptoPricesTopic() {
        return TopicBuilder.name(CRYPTO_PRICES_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic priceUpdatesTopic() {
        return TopicBuilder.name(PRICE_UPDATES_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}