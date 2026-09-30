package tripwire.tripwire;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class HealthCheckRunner implements CommandLineRunner{
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String TEST_TOPIC = "health-check-topic";

    @Override
    public void run(String... args) throws Exception {
        // Test Redis: write and read a value
        redisTemplate.opsForValue().set("health-check", "redis-is-alive");
        String redisResult = redisTemplate.opsForValue().get("health-check");
        System.out.println("✅ Redis check: " + redisResult);

        // Test Kafka: send a message (consumer below will pick it up)
        kafkaTemplate.send(TEST_TOPIC, "kafka-is-alive");
        System.out.println("✅ Kafka message sent, waiting for consumer...");
    }

    @KafkaListener(topics = TEST_TOPIC, groupId = "health-check-group")
    public void listen(String message) {
        System.out.println("✅ Kafka check: received message -> " + message);
    }
    
}
