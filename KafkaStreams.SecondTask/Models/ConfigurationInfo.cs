namespace KafkaStreams.SecondTask.Models;

public record ConfigurationInfo(string ApplicationId, string BootstrapServers, ConsumerInfo From);