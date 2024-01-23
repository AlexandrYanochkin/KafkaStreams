namespace KafkaStreams.FourthTask.Models;

public record ConfigurationInfo(string ApplicationId, string BootstrapServers, ConsumerInfo From);