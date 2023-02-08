package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

public class BasicConsumer {
    public static void main(String[] args) {
        final var topic = "topic1";
        final Map<String, Object> configuration = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.GROUP_ID_CONFIG, "group-1", // menetapkan consumer group
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // equal to --from-beginning params
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false // mematikan fitur auto commit setiap 5s
        );
        try(
                var consumer = new KafkaConsumer<>(configuration);
                ){
            consumer.subscribe(Set.of(topic)); // Consumer subscribe ke topic "topic1"

            while(true){
                final var records = consumer.poll(Duration.ofMillis(1000)); // consume data
                for(var record: records){
                    System.out.format("Mendapatkan record dengan nilai = %s\n", record.value());
                }
                consumer.commitAsync(); // Commit secara manual
            }
        }

    }
}
