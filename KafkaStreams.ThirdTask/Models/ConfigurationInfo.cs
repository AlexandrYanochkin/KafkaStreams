namespace KafkaStreams.ThirdTask.Models;

public record ConfigurationInfo(string ApplicationId, string BootstrapServers, ConsumerInfo From);