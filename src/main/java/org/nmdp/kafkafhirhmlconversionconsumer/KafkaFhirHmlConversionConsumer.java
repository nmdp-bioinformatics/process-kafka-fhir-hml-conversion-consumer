package org.nmdp.kafkafhirhmlconversionconsumer;

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

import java.net.URL;

import org.nmdp.kafkafhirhmlconversionconsumer.kafka.KafkaFhirHmlConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaFhirHmlConversionConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaFhirHmlConversionConsumer.class);

    public static void main(String[] args) throws Throwable {
        KafkaFhirHmlConverter khwb = null;
        URL consumerConfigUrl;

        try {
            consumerConfigUrl = new URL("file:." + "/src/main/resources/consumer-configuration.yaml");
            khwb = new KafkaFhirHmlConverter(consumerConfigUrl);
        } catch (Exception ex) {
            LOG.error("Exception thrown: " + ex + "\nShutting Down.");
            khwb.shutdown();
        }
    }
}
