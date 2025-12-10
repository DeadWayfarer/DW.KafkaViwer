namespace DW.KafkaViwer.Web.Models;

public record BrokerInfo(
    int Id,
    string ConnectionName,
    string Host,
    int Port,
    string Status,
    string? ClientId = null,
    string? ClientSecret = null,
    string? OIDCEndpoint = null);

