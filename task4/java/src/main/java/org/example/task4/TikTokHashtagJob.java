package org.example.task4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Map;

public class TikTokHashtagJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(TikTokHashtagJob.class);

    private static final DateTimeFormatter ISO_FORMAT_3 = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
            .optionalEnd()
            .appendPattern("XXX")
            .toFormatter(Locale.ROOT);


    private static long parseIsoMillis(String s) {
        if (s == null) return -1L;
        s = s.trim().replace("\"", "");
        try {
            return Instant.from(ISO_FORMAT_3.parse(s)).toEpochMilli();
        } catch (Exception ignore) {
            LOG.warn("Failed to parse ISO date with millis: " + s);
        }
        return -1L;
    }

    private static boolean containsHashtag(Map<String, Object> record, String hashtag) {
        String tagNorm = hashtag.replace("#", "").toLowerCase(Locale.ROOT);

        Object h = record.get("comment_text");
        if (h instanceof String) {
            String hs = ((String) h).trim();
            if (!hs.equalsIgnoreCase("null") && !hs.isEmpty()) {
                return hs.toLowerCase(Locale.ROOT).contains(tagNorm);
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = getenv("KAFKA_BROKER", "kafka-broker:9092");
        String topic = getenv("TIKTOK_TOPIC", "tiktok_posts");
        String groupId = getenv("TIKTOK_GROUP", "flink-tiktok-hashtag-java");
        String hashtag = getenv("HASHTAG", "SAE");
        int windowSec = Integer.parseInt(getenv("WINDOW_SEC", "15"));
        int latenessSec = Integer.parseInt(getenv("WATERMARK_LATENESS_SEC", "5"));
        
        LOG.info("Starting TikTokHashtagJob with config: bootstrap={}, topic={}, groupId={}, hashtag=#{} windowSec={}s, wmLateness={}s",
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

        // Create watermark strategy with proper timestamp extraction from JSON
        WatermarkStrategy<String> wmStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(latenessSec))
                .withIdleness(Duration.ofSeconds(10))  // Close windows if no data for 10 seconds
                .withTimestampAssigner((element, recordTimestamp) -> {
                    Map<String, Object> m = null;
                    try {
                        m = MAPPER.readValue(element, new TypeReference<Map<String, Object>>() {});
                        Object dateCreated = m.get("date_created");
                        if (dateCreated instanceof String) {
                            return parseIsoMillis((String) dateCreated);
                        }
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    return -2;
                });

        // Apply watermark strategy directly when creating the source stream
        DataStream<String> raw = env.fromSource(source, wmStrategy, "tiktok-source");

        // Parse JSON and filter out invalid records using flatMap
        SingleOutputStreamOperator<Map<String, Object>> mapped = raw
                .flatMap((String s, Collector<Map<String, Object>> out) -> {
                    try {
                        Map<String, Object> m = MAPPER.readValue(s, new TypeReference<Map<String, Object>>() {});
                        // Early filtering: only emit if contains the hashtag
                        if (m != null && !m.isEmpty() && containsHashtag(m, hashtag)) {
                            out.collect(m);
                        }
                    } catch (Exception e) {
                        String preview = s == null ? "null" : (s.length() > 200 ? s.substring(0, 200) + "..." : s);
                        LOG.warn("Failed to parse JSON message, skipping. Preview='{}'", preview, e);
                    }
                })
                .returns(new TypeHint<Map<String, Object>>() {})
                .name("filter-hashtag");

        // Map to count (just emit 1 for each matching record)
        SingleOutputStreamOperator<Integer> counts = mapped
                .map(m -> 1)
                .returns(Integer.class)
                .name("map-to-one");

        // Key by a constant to enable windowing (all records go to same key)
        KeyedStream<Integer, String> keyed = counts.keyBy(val -> hashtag);

        // Apply tumbling window and sum counts
        SingleOutputStreamOperator<Integer> windowed = keyed
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSec)))
                .reduce(Integer::sum)
                .name("window-aggregate");

        // Format output and print
        windowed
                .map(count -> {
                    String out = String.format("[TikTokHashtagCount] hashtag=#%s window=%ds count=%d", 
                            hashtag, windowSec, count);
                    LOG.info(out);
                    return out;
                })
                .returns(String.class)
                .name("format-output")
                .print();

        LOG.info("Submitting Flink job to execute...");
        env.execute("TikTok Hashtag Count Java #" + hashtag);
        LOG.info("Flink job has terminated");
    }

    private static String getenv(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }
}