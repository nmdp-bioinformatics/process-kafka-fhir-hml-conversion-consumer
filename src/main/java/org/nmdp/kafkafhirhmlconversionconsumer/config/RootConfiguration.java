package org.nmdp.kafkafhirhmlconversionconsumer.config;

/**
 * Created by Andrew S. Brown, Ph.D., <andrew@nmdp.org>, on 6/6/17.
 * <p>
 * process-kafka-fhir-hml-conversion-consumer
 * Copyright (c) 2012-2017 National Marrow Donor Program (NMDP)
 * <p>
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; either version 3 of the License, or (at
 * your option) any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; with out even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library;  if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA.
 * <p>
 * > http://www.fsf.org/licensing/licenses/lgpl.html
 * > http://www.opensource.org/licenses/lgpl-license.php
 */

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.nmdp.kafkaconsumer.consumer.KafkaMessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class RootConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(RootConfiguration.class);

    private String consumerGroupPrefix;
    private long allowedConsumerDelayMs = 60000L;
    private long healthReportingIntervalMs = 60000L;

    private final Map<String, String> consumerDefaults = new HashMap<>();
    private final List<KafkaBrokerConfiguration> kafkaBrokers = new ArrayList<>();
    private final List<ConnectorConfiguration> connectors = new ArrayList<>();

    public void setConsumerGroupPrefix(String consumerGroupPrefix) {
        this.consumerGroupPrefix = consumerGroupPrefix;
    }

    public String getConsumerGroupPrefix() {
        return consumerGroupPrefix;
    }

    public long getAllowedConsumerDelayMs() {
        return allowedConsumerDelayMs;
    }

    public void setAllowedConsumerDelayMs(long allowedConsumerDelayMs) {
        this.allowedConsumerDelayMs = allowedConsumerDelayMs;
    }

    public long getHealthReportingIntervalMs() {
        return healthReportingIntervalMs;
    }

    public void setHealthReportingIntervalMs(long healthReportingIntervalMs) {
        this.healthReportingIntervalMs = healthReportingIntervalMs;
    }

    public void setConsumerDefaults(Map<String, String> consumerDefaults) {
        Objects.requireNonNull(consumerDefaults);
        this.consumerDefaults.clear();
        this.consumerDefaults.putAll(consumerDefaults);
    }

    public Map<String, String> getConsumerDefaults() {
        return Collections.unmodifiableMap(consumerDefaults);
    }

    public List<KafkaBrokerConfiguration> getKafkaBrokers() {
        return kafkaBrokers;
    }

    public void setKafkaBrokers(List<KafkaBrokerConfiguration> kafkaBrokers) {
        Objects.requireNonNull(kafkaBrokers);
        this.kafkaBrokers.clear();
        this.kafkaBrokers.addAll(kafkaBrokers);
    }

    public List<ConnectorConfiguration> getConnectors() {
        return Collections.unmodifiableList(connectors);
    }

    public void setConnectors(List<ConnectorConfiguration> connectors) {
        Objects.requireNonNull(connectors);
        this.connectors.clear();
        this.connectors.addAll(connectors);
    }

    public Map<String, List<KafkaMessageConsumer>> initConnectors(List<Closeable> closeables, MetricRegistry metrics)
            throws Exception {
        Map<String, KafkaBrokerConfiguration> kafkaConfigMap = kafkaBrokers.stream()
                .collect(Collectors.toMap(KafkaBrokerConfiguration::getId, r -> r));

        Map<String, List<KafkaMessageConsumer>> connectorInstances = new HashMap<>();
        for (ConnectorConfiguration conf : connectors) {
            List<KafkaMessageConsumer> connector = conf.createConnectors(
                    closeables, metrics, kafkaConfigMap, consumerDefaults, consumerGroupPrefix);

            if (connectorInstances.put(conf.getId(), connector) != null) {
                throw new IllegalArgumentException("Duplicate connector with ID " + conf.getId());
            }
        }
        LOG.info("Connectors: {}", connectorInstances);
        return connectorInstances;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
