namespace DW.KafkaViwer.Web.Models;

public record TopicMessageFilter(string TopicName);

public record TopicMessageInfo(
    string Topic,
    int Partition,
    long Offset,
    string Key,
    string Value,
    DateTime TimestampUtc);

