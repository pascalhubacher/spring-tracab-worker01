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

    stream.mapValues(valueFrame ->
            singletonList.getInstance().calculate(valueFrame))
        .to(topicOut);

    return stream;

  }
}


