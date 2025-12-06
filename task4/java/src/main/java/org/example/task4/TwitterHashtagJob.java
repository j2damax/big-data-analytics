package org.example.task4;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TwitterHashtagJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(TwitterHashtagJob.class);

    private static boolean containsHashtag(Map<String, Object> record, String hashtag) {
        if (record == null || record.isEmpty()) return false;

        String tagNorm = hashtag.replace("#", "").toLowerCase(Locale.ROOT);

        // 1) Check tweet text/description
        Object desc = record.get("description");
        if (desc instanceof String) {
            String s = ((String) desc).trim();
            if (!s.equalsIgnoreCase("null") && !s.isEmpty()) {
                if (s.toLowerCase(Locale.ROOT).contains(tagNorm)) {
                    return true;
                }
            }
        }

        // 2) Check hashtags array field (often JSON string array in the csv)
        Object h = record.get("hashtags");
        if (h instanceof String) {
            String hs = ((String) h).trim();
            if (!hs.equalsIgnoreCase("null") && !hs.isEmpty()) {
                try {
                    List<String> arr = MAPPER.readValue(hs, new TypeReference<List<String>>() {});
                    for (String it : arr) {
                        if (it != null && !it.isEmpty()) {
                            if (it.replace("#", "").toLowerCase(Locale.ROOT).equals(tagNorm)) {
                                return true;
                            }
                        }
                    }
                } catch (Exception ignore) {
                    // Not a JSON array; fallback to substring match
                    if (hs.toLowerCase(Locale.ROOT).contains(tagNorm)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = getenv("KAFKA_BROKER", "kafka-broker:9092");
        String topic = getenv("TWITTER_TOPIC", "twitter_posts");
        String groupId = getenv("TWITTER_GROUP", "flink-twitter-hashtag-java");
        String hashtag = getenv("HASHTAG", "SAE");
        int windowSec = Integer.parseInt(getenv("WINDOW_SEC", "15"));
        int latenessSec = Integer.parseInt(getenv("WATERMARK_LATENESS_SEC", "5"));

        LOG.info("Starting TwitterHashtagJob with config: bootstrap={}, topic={}, groupId={}, hashtag=#{} windowSec={}s, wmLateness={}s",
                bootstrap, topic, groupId, hashtag, windowSec, latenessSec);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        WatermarkStrategy<String> wmStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(latenessSec))
                .withIdleness(Duration.ofSeconds(10))
                .withTimestampAssigner((element, recordTimestamp) -> recordTimestamp); // use Kafka record ts

        DataStream<String> raw = env.fromSource(source, wmStrategy, "twitter-source");

        // Parse JSON rows and filter out invalid ones
        SingleOutputStreamOperator<Map<String, Object>> mapped = raw
                .flatMap((String s, Collector<Map<String, Object>> out) -> {
                    try {
                        Map<String, Object> m = MAPPER.readValue(s, new TypeReference<Map<String, Object>>() {});
                        out.collect(m);
                    } catch (Exception e) {
                        String preview = s == null ? "null" : (s.length() > 200 ? s.substring(0, 200) + "..." : s);
                        LOG.warn("Failed to parse JSON message, skipping. Preview='{}'", preview, e);
                    }
                })
                .returns(new TypeHint<Map<String, Object>>() {})
                .name("to-json-map");

        // Map to (hashtag, count)
        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedOnConst = mapped
                .map(m -> Tuple2.of(hashtag, containsHashtag(m, hashtag) ? 1 : 0))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .name("check-hashtag");

        KeyedStream<Tuple2<String, Integer>, String> keyed = keyedOnConst.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> windowed = keyed
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1))
                .name("window-aggregate");

        windowed
                .map(kv -> {
                    String out = String.format("[TwitterHashtagCount] hashtag=#%s window=%ds count=%d",
                            hashtag, windowSec, kv.f1);
                    LOG.info(out);
                    return out;
                })
                .returns(String.class)
                .name("format-output")
                .print();

        LOG.info("Submitting Flink job to execute...");
        env.execute("Twitter Hashtag Count Java #" + hashtag);
        LOG.info("Flink job has terminated");
    }

    private static String getenv(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }
}
