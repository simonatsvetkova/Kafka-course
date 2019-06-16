package course.kafka;

import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static course.kafka.StockPriceConstants.PRICES_TOPIC;

@Slf4j

public class StockPriceConsumer {

    private Properties props = new Properties();
    private KafkaConsumer<String, StockPrice> consumer;
    private Map<String, Integer> eventMap = new ConcurrentHashMap<>();
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private int count = 0;


    public StockPriceConsumer() {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "stock-price-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "course.kafka.serialization.JsonDeserializer");
        props.put("value.deserializer.class", "course.kafka.model.StockPrice");
//        props.put("enable.auto.commit", "true");

        consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(PRICES_TOPIC),
                new StockPriceRebalanceListener(consumer));
//        consumer.poll(Duration.ofMillis(100));
//        consumer.seekToBeginning(Arrays.asList(
//                new TopicPartition(PRICES_TOPIC, 0),
//                new TopicPartition(PRICES_TOPIC, 1),
//                new TopicPartition(PRICES_TOPIC, 2)
//        ));
        try {

            while (true) {
                ConsumerRecords<String, StockPrice> records = consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                    log.info("Fetched {} records", records.count());
                    for (ConsumerRecord<String, StockPrice> rec : records) {
                        log.debug("Record - topic {}, partition {}, offset {}, timestamp {}, value: {}",
                                rec.topic(),
                                rec.partition(),
                                rec.offset(),
                                rec.timestamp(),
                                rec.value());
                        log.info("{} -> {}, {}, {}, {}",
                                rec.key(),
                                rec.value().getId(),
                                rec.value().getSymbol(),
                                rec.value().getName(),
                                rec.value().getPrice());
                        int updatedCount = 1;
                        if (eventMap.containsKey(rec.key())) {
                            updatedCount = eventMap.get(rec.key()) + 1;
                        }
                        eventMap.put(rec.key(), updatedCount);

                        //update current offsets

                        currentOffsets.put(new TopicPartition(rec.topic(), rec.partition()), new OffsetAndMetadata(rec.offset() + 1));


                        if (++count % 3 == 0) {
                            commitOffsets();
                        }


                    }
                    commitOffsets();

                    JSONObject json = new JSONObject(eventMap);
                    log.info(json.toJSONString());
                }
            }
        } catch (Exception e) {
            log.error("Error polling data.");
        } finally {
            try {
                consumer.commitSync();

            } finally {
                consumer.close();
            }

        }
    }

    private void commitOffsets() {
        consumer.commitAsync(currentOffsets, (offsets, exception) -> {
            if (exception != null) {
                log.error("Error commiting offsets: {}, exception: {}", offsets, exception);
                return;
            }
            log.debug("Offsets commited: {}", offsets);
        });
    }

    public static void main(String[] args) {
        StockPriceConsumer consumer = new StockPriceConsumer();
        consumer.run();

    }

}
