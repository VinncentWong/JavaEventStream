package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Map;

public class BasicProducer {
    public static void main(String[] args) throws InterruptedException{
        final String topic = "topic1";
        final Map<String, Object> configuration = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", // lokasi broker
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true // memastikan tidak ada duplikasi atau out-of-order record
        );

        try(
           var producer = new KafkaProducer<String, String>(configuration);
           ) {
            while(true){
                final String key = "key";
                final String value = new Date().toString();
                System.out.format("Publish record dengan nilai %s\n", value);

                // Callback function akan dipanggil tepat setelah berhasil menyimpan record di broker
                final Callback callback = ((metadata, exception) -> {
                   System.out.format("Publish record dengan metadata = %s, error = %s\n", metadata, exception);
                });

                producer.send(new ProducerRecord<>(topic, key, value), callback);

                // Thread akan berhenti dieksekusi selama 1000ms sebelum publish selanjutnya
                Thread.sleep(1000);
            }
        }
    }
}
