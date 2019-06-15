package course.kafka;

import course.kafka.model.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@Slf4j

public class DemoProducer {
    private Properties kafkaProps = new Properties();
    private Producer producer;

    public DemoProducer() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "course.kafka.serialization.JsonSerializer");
        kafkaProps.put("acks", "all");
        kafkaProps.put("enable.idempotence", "true"); // strategy #1
        kafkaProps.put("max.request.size", "160");
        kafkaProps.put("transactional.id", "event-producer-1");
        kafkaProps.put("partitioner.class", "course.kafka.partitioner.SimplePartitioner");

        producer = new KafkaProducer<String, Customer>(kafkaProps);
    }


    public void run() {
        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {

                Customer cust = new Customer(i,"ABC Ltd." + i ,"12345678" + i, "Sofia 100" + i);

                ProducerRecord<String, Customer> record = new ProducerRecord<>("events-replicated", "" + i, cust);




                Future<RecordMetadata> futureResult = producer.send(record, ((metadata, exception) -> {
                    if (exception != null) {
                        log.error("Error while producing", exception);
                    }
                    log.info("topic: {}, partition {}, offset {}, timestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                }));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException ex) {
            producer.close();
        } catch (KafkaException ex1) {
            producer.abortTransaction(); // similar to rollback
        }


        producer.close();
    }


    public static void main(String[] args) throws InterruptedException {
        DemoProducer producer = new DemoProducer();
        producer.run();
        Thread.sleep(5000);
    }
}