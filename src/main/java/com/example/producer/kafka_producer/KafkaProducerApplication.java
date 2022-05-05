package com.example.producer.kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.tomcat.util.json.JSONParser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

//@SpringBootApplication
public class KafkaProducerApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
//		String topicName = "dit_cpm_ps_retail.pim.event.retail_product_data";
		String topicName = "test-kafka";

		String server = "localhost:9092";

		final Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				server);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		final Producer<Long, String> producer =
				new KafkaProducer<>(props);

		String fileName = "src/main/resources/data/sample_kafka_message_last.json";
		String message = Files.readString(Paths.get(fileName));


		RecordMetadata recordMetadata = (RecordMetadata) producer.send(new ProducerRecord(topicName, message)).get();
		if (recordMetadata.hasOffset())
			System.out.println("Message sent successfully");

		producer.close();

//		SpringApplication.run(KafkaProducerApplication.class, args);
	}

//	private void parseJsonFile(){
//		JSONParser parser = new JSONParser();
//
//		try {
//			Object obj = parser.parse(new FileReader("c:\\file.json"));
//
//			JSONObject jsonObject =  (JSONObject) obj;
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//	}




}
