import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Productor {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("acks", "all");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  "localhost:29092,localhost:9093,localhost:9094");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("buffer.memory", 33554432);

    KafkaProducer<String, String> prod = new KafkaProducer<>(props);//inicializando el buffer de kafka
    String topic = "topic-test";
    //int partition = 0;

    for (int i = 0; i <= 10; i++) {

      String key = "testKey-" + i;
      String value = "message to the topic: " + i;
      //prod.send(new ProducerRecord<>(topic,key, value));//envio asincrono
      final ProducerRecord<String, String> record = new ProducerRecord<>(topic,key, value);
      prod.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (e != null){
            System.out.println("send failed for record");
          }
        }
      });//envio sincrono

    }
    //prod.flush();
    prod.close();//cerrar el envio de mensajes al productor

  }

}
