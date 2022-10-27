package com.mas2022datascience.springtracabworker01.processor;

import com.mas2022datascience.avro.v1.Frame;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {
  @Value(value = "${topic.tracab-01.name}")
  private String topicIn;

  @Value(value = "${topic.tracab-02.name}")
  private String topicOut;

  @Bean
  public KStream<String, Frame> kStream(StreamsBuilder kStreamBuilder) {

//    // When configuring the default serdes of StreamConfig
//    final Properties streamsConfiguration = new Properties();
//    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
//    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
//    streamsConfiguration.put("schema.registry.url", "http://my-schema-registry:8081");
//
//// When you want to override serdes explicitly/selectively
//    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
//        "http://my-schema-registry:8081");
//// `Foo` and `Bar` are Java classes generated from Avro schemas
//    final Serde<Foo> keySpecificAvroSerde = new SpecificAvroSerde<>();
//    keySpecificAvroSerde.configure(serdeConfig, true); // `true` for record keys
//    final Serde<Bar> valueSpecificAvroSerde = new SpecificAvroSerde<>();
//    valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values
//
//    StreamsBuilder builder = new StreamsBuilder();
//    KStream<Foo, Bar> textLines = builder.stream("my-avro-topic", Consumed.with(keySpecificAvroSerde, valueSpecificAvroSerde));
//
//
    KStream<String, Frame> stream = kStreamBuilder.stream(topicIn);
//    stream.foreach(
//        (key, value) -> {
//          System.out.println("(From DSL) " + value.getUtc());
//        });

    return stream;

  }

}
