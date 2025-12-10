namespace DW.KafkaViwer.Web.Models;

public record BrokerInfo(
    int Id,
    string Host,
    int Port,
    string Status);

