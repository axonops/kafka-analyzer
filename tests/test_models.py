from kafka_analyzer.models import Partition, Topic


def test_partition_under_replicated():
    p = Partition(id=0, leader=1, replicas=[1, 2, 3], in_sync_replicas=[1, 2])
    assert p.is_under_replicated is True
    assert p.is_offline is False


def test_partition_offline():
    p = Partition(id=0, leader=-1, replicas=[1, 2, 3], in_sync_replicas=[])
    assert p.is_offline is True


def test_topic_retention_ms():
    t = Topic(name="t", configs={"retention.ms": "604800000"})
    assert t.retention_ms() == 604_800_000


def test_topic_min_insync_replicas_missing():
    t = Topic(name="t", configs={})
    assert t.min_insync_replicas() is None


def test_topic_internal_detection():
    assert Topic(name="__consumer_offsets").is_system_topic is True
    assert Topic(name="orders").is_system_topic is False


def test_topic_message_count():
    p = Partition(id=0, low_water_mark=10, high_water_mark=42)
    assert p.message_count == 32
