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

    KStream<String, Frame> stream = kStreamBuilder.stream(topicIn);

//    KTable<String, Frame> frame = stream.toTable(Materialized.as("test"));

//    InMemoryKeyValueStore<String, Object> keyValueStore =
//        streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());

//    KTable<String, Arrays<Integer>> objects = stream.

//  <Frame utc="2019-06-05T18:47:25.843" isBallInPlay="1" ballPossession="Away">
//    <Objs>
//      <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
//      <Obj type="1" id="97746" x="4847" y="630" sampling="0" />
//      <Obj type="1" id="250079677" x="1439" y="2441" sampling="0" />
//  key: id
//  value: [x, y, z]


    stream.mapValues(valueFrame ->
            singletonList.getInstance().getVelocity(valueFrame))
        .to(topicOut);
    // publish to the output topic
//    KStream<String, Frame> velocityStream = stream.
    //KStream<Void, String> upperStream = stream.mapValues(value -> value.toUpperCase());
    //upperStream.to(topicOut);

    return stream;

  }
}


