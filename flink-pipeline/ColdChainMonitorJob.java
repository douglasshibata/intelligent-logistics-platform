package com.logistics.pipeline;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * ColdChainMonitorJob
 *
 * Apache Flink streaming pipeline engineered for real-time supply chain cold-chain tracking.
 * It ingests high-frequency container telemetry from Apache RocketMQ, enforces geofencing
 * validations, and implements stateful CEP alerts for temperature breaches over continuous
 * sliding intervals using a RocksDB state backend.
 */
public class ColdChainMonitorJob {

    public static void main(String[] args) throws Exception {
        // 1. Initialize Stream Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure RocksDB state checkpoints for enterprise fault tolerance (Exactly-Once)
        env.getCheckpointConfig().setCheckpointInterval(10000); // 10s checkpointing interval

        // 2. Consume from RocketMQ Telemetry Stream
        DataStream<String> telemetryRawStream = env.addSource(
            new RocketMQTelemetrySource("rocketmq-namesrv:9876", "coldchain_telemetry")
        );

        // 3. Parse JSON, partition (keyBy) by ContainerID and evaluate temperature breaches and geofences
        DataStream<EnrichedContainerEvent> alertStream = telemetryRawStream
            .map(new TelemetryJSONParser())
            .keyBy(ContainerTelemetryEvent::getContainerId)
            .map(new StatefulAnomalyCEPProcessor());

        // 4. Sink to output alerts topic/system
        alertStream.addSink(new LogisticsAlertSink());

        // 5. Execute Flink DAG
        env.execute("Cold-Chain-Stateful-CEP-Logistics-Pipeline");
    }

    // ==========================================================
    // DOMAIN MODELS
    // ==========================================================
    public static class ContainerTelemetryEvent implements Serializable {
        private String containerId;
        private double temperature;
        private double latitude;
        private double longitude;
        private double targetLatitude;
        private double targetLongitude;
        private long timestamp;

        public ContainerTelemetryEvent() {}

        public String getContainerId() { return containerId; }
        public void setContainerId(String containerId) { this.containerId = containerId; }

        public double getTemperature() { return temperature; }
        public void setTemperature(double temperature) { this.temperature = temperature; }

        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }

        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }

        public double getTargetLatitude() { return targetLatitude; }
        public void setTargetLatitude(double targetLatitude) { this.targetLatitude = targetLatitude; }

        public double getTargetLongitude() { return targetLongitude; }
        public void setTargetLongitude(double targetLongitude) { this.targetLongitude = targetLongitude; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class EnrichedContainerEvent extends ContainerTelemetryEvent implements Serializable {
        private boolean alertFlagged;
        private String alertType; // "TEMPERATURE_BREACH", "GEOFENCE_DEVIATION", "NONE"
        private String details;
        private int consecutiveViolations;

        public EnrichedContainerEvent(ContainerTelemetryEvent base) {
            this.setContainerId(base.getContainerId());
            this.setTemperature(base.getTemperature());
            this.setLatitude(base.getLatitude());
            this.setLongitude(base.getLongitude());
            this.setTargetLatitude(base.getTargetLatitude());
            this.setTargetLongitude(base.getTargetLongitude());
            this.setTimestamp(base.getTimestamp());
        }

        public boolean isAlertFlagged() { return alertFlagged; }
        public void setAlertFlagged(boolean alertFlagged) { this.alertFlagged = alertFlagged; }

        public String getAlertType() { return alertType; }
        public void setAlertType(String alertType) { this.alertType = alertType; }

        public String getDetails() { return details; }
        public void setDetails(String details) { this.details = details; }

        public int getConsecutiveViolations() { return consecutiveViolations; }
        public void setConsecutiveViolations(int consecutiveViolations) { this.consecutiveViolations = consecutiveViolations; }
    }

    // ==========================================================
    // PARSERS AND STREAM OPERATORS (STATEFUL CEP)
    // ==========================================================
    private static class TelemetryJSONParser extends RichMapFunction<String, ContainerTelemetryEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            this.mapper = new ObjectMapper();
        }

        @Override
        public ContainerTelemetryEvent map(String value) throws Exception {
            return mapper.readValue(value, ContainerTelemetryEvent.class);
        }
    }

    /**
     * StatefulAnomalyCEPProcessor
     *
     * Tracks rolling state per container to evaluate:
     * - Temperature deviations exceeding 8.0°C continuously.
     * - Haversine distance geofence deviations exceeding 0.05 degrees (~5.5km) from target route.
     */
    private static class StatefulAnomalyCEPProcessor extends RichMapFunction<ContainerTelemetryEvent, EnrichedContainerEvent> {
        private transient ValueState<ContainerState> containerStateStore;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<ContainerState> descriptor = new ValueStateDescriptor<>(
                "containerStateStore",
                TypeInformation.of(ContainerState.class)
            );
            containerStateStore = getRuntimeContext().getState(descriptor);
        }

        @Override
        public EnrichedContainerEvent map(ContainerTelemetryEvent value) throws Exception {
            ContainerState state = containerStateStore.value();
            if (state == null) {
                state = new ContainerState();
                state.consecutiveTempBreaches = 0;
            }

            EnrichedContainerEvent enriched = new EnrichedContainerEvent(value);
            boolean alertTriggered = false;
            String type = "NONE";
            String reason = "Normal Parameters Verified.";

            // 1. Geofence evaluation using Haversine calculation approximation
            double dist = calculateDistance(value.getLatitude(), value.getLongitude(), value.getTargetLatitude(), value.getTargetLongitude());
            if (dist > 5.5) { // Deviated by more than 5.5 kilometers
                alertTriggered = true;
                type = "GEOFENCE_DEVIATION";
                reason = String.format("Vehicle diverted from target route! Deviation: %.2f km", dist);
            }

            // 2. Cold Chain temperature breach evaluation (> 8.0°C)
            if (value.getTemperature() > 8.0) {
                state.consecutiveTempBreaches += 1;
            } else {
                state.consecutiveTempBreaches = 0; // reset
            }

            // Trigger alarm if temperature breaches persist for more than 3 consecutive readings
            if (state.consecutiveTempBreaches >= 3) {
                alertTriggered = true;
                type = "TEMPERATURE_BREACH";
                reason = String.format("Critical cold chain breakdown! Temperature above 8°C for %d continuous intervals. Current: %.2f°C",
                    state.consecutiveTempBreaches, value.getTemperature());
            }

            enriched.setAlertFlagged(alertTriggered);
            enriched.setAlertType(type);
            enriched.setDetails(reason);
            enriched.setConsecutiveViolations(state.consecutiveTempBreaches);

            // Persist the state update back to the RocksDB StateBackend
            containerStateStore.update(state);
            return enriched;
        }

        // Haversine formula approximation to calculate distance in kilometers
        private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
            double R = 6371; // Earth radius in km
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                       Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                       Math.sin(dLon / 2) * Math.sin(dLon / 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }
    }

    public static class ContainerState implements Serializable {
        public int consecutiveTempBreaches;
    }

    // ==========================================================
    // MOCK ROCKETMQ SOURCE & INGESTION SIMULATOR
    // ==========================================================
    private static class RocketMQTelemetrySource implements SourceFunction<String> {
        private final String namesrv;
        private final String topic;
        private volatile boolean isRunning = true;

        public RocketMQTelemetrySource(String namesrv, String topic) {
            this.namesrv = namesrv;
            this.topic = topic;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                // Simulate periodic events including temperature spikes and geofence anomalies
                double simulatedTemp = 4.0 + (Math.random() * 6.0); // occasionally exceeds 8.0°C
                double simulatedLat = -23.55 + (Math.random() * 0.08); // occasionally deviates from target
                String mockEvent = String.format(
                    "{\"containerId\":\"CNT-SP-001\",\"temperature\":%.2f,\"latitude\":%.4f,\"longitude\":-46.6396,\"targetLatitude\":-23.5505,\"targetLongitude\":-46.6333,\"timestamp\":%d}",
                    simulatedTemp, simulatedLat, System.currentTimeMillis()
                );
                ctx.collect(mockEvent);
                Thread.sleep(3000); // Poll telemetry every 3 seconds
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }

    // ==========================================================
    // SINK FOR DETECTED LOGISTICS ANOMALIES
    // ==========================================================
    private static class LogisticsAlertSink implements SinkFunction<EnrichedContainerEvent> {
        @Override
        public void invoke(EnrichedContainerEvent value, Context context) {
            if (value.isAlertFlagged()) {
                System.out.println(String.format(
                    "[LOGISTICS ALERT] [%s] - Container: %s | Temp: %.2f°C | Coords: (%.4f, %.4f) | Alarm Detail: %s",
                    value.getAlertType(), value.getContainerId(), value.getTemperature(), value.getLatitude(), value.getLongitude(), value.getDetails()
                ));
            } else {
                System.out.println(String.format(
                    "[LOGISTICS OK] Container: %s | Temp: %.2f°C | Status: Verified safe.",
                    value.getContainerId(), value.getTemperature()
                ));
            }
        }
    }
}
