package com.mas2022datascience.springtracabworker01.processor;

import com.mas2022datascience.avro.v1.Frame;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
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

//    final Serde<Object> objectSerde = new SpecificAvroSerde<>();
    final Serde<Frame> frameSerde = new SpecificAvroSerde<>();

    KStream<String, Frame> stream = kStreamBuilder.stream(topicIn);

//    final StoreBuilder<KeyValueStore<String, Object>> myStateStore = Stores
//        .keyValueStoreBuilder(Stores.persistentKeyValueStore("MyStateStore"),
//            Serdes.String(), objectSerde)
//        .withCachingEnabled();
//    builder.addStateStore(myStateStore);
//
//    final MyStateHandler myStateHandler = new MyStateHandler(myStateStore.name());

    stream
        .mapValues(valueFrame ->
            singletonList.getInstance().calculate(valueFrame))
        .to(topicOut);

//    // invoke the transformer
//    KStream<String, Frame> transformedStream = stream.transform(() -> myStateHandler, myStateStore.name());
//
//    // peek into the stream and execute a println
//    //transformedStream.peek((k,v) -> System.out.println("key: " + k + " - value:" + v));
//
//    // publish result
//    //transformedStream.to("test-kstream-output-topic");

    return stream;

  }

//  private static final class MyStateHandler implements
//      Transformer<String, Object, KeyValue<String, Object>> {
//    final private String storeName;
//    private KeyValueStore<String, Object> stateStore;
//    private ProcessorContext context;
//
//    public MyStateHandler(final String storeName) {
//      this.storeName = storeName;
//    }
//
//    @Override
//    public void init(ProcessorContext processorContext) {
//      this.context = processorContext;
//      stateStore = (KeyValueStore<String, Object>) this.context.getStateStore(storeName);
//    }
//
//    @Override
//    public KeyValue<String, Object> transform(String key, Object value) {
//      stateStore.put(key, value);
//      return new KeyValue<>(key, stateStore.get(key));
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//  }

}


