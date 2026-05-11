"""
plugins/kafka_sensor.py
Custom Airflow Sensor: polls a Kafka consumer group's lag
and succeeds when all topics have been consumed below the threshold.
"""

from typing import List, Optional
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class KafkaTopicSensor(BaseSensorOperator):
    """
    Waits until the consumer group lag for the given topics
    drops below `max_lag_threshold`.

    :param kafka_bootstrap_servers: Kafka bootstrap server string
    :param topics:                  List of Kafka topic names to monitor
    :param group_id:                Consumer group ID to check lag for
    :param max_lag_threshold:       Maximum allowable lag per topic partition
    """

    template_fields = ("kafka_bootstrap_servers", "topics")
    ui_color = "#f0ffe0"

    @apply_defaults
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topics: List[str],
        group_id: str = "airflow-sensor",
        max_lag_threshold: int = 5_000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topics                  = topics
        self.group_id                = group_id
        self.max_lag_threshold       = max_lag_threshold

    def poke(self, context) -> bool:
        """
        Query Kafka AdminClient for consumer group offsets vs. end offsets.
        Returns True when total lag <= max_lag_threshold for all topics.
        """
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": self.kafka_bootstrap_servers})

        # Fetch end offsets (high watermarks) for all topic partitions
        consumer = Consumer({
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id":          self.group_id,
            "enable.auto.commit":False,
        })

        total_lag = 0

        try:
            for topic in self.topics:
                # Get metadata to find partition count
                metadata = consumer.list_topics(topic=topic, timeout=10)
                if topic not in metadata.topics:
                    self.log.warning("Topic %s not found in Kafka.", topic)
                    return False

                partitions = [
                    TopicPartition(topic, pid)
                    for pid in metadata.topics[topic].partitions.keys()
                ]

                # Get committed offsets for the consumer group
                committed = consumer.committed(partitions, timeout=10)

                for tp in committed:
                    # Get end offset (high watermark)
                    _, high = consumer.get_watermark_offsets(tp, timeout=10)
                    committed_offset = tp.offset if tp.offset >= 0 else 0
                    lag = max(0, high - committed_offset)
                    total_lag += lag
                    self.log.debug(
                        "Topic=%s Partition=%d Committed=%d High=%d Lag=%d",
                        tp.topic, tp.partition, committed_offset, high, lag,
                    )

        finally:
            consumer.close()

        self.log.info(
            "Total consumer group lag: %d (threshold: %d)",
            total_lag, self.max_lag_threshold,
        )

        return total_lag <= self.max_lag_threshold
