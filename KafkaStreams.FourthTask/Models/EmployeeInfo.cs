namespace KafkaStreams.FourthTask.Models;

public record EmployeeInfo
{
    public string Name { get; init; }
    public string Company { get; init; }
    public string Position { get; init; }
    public short Experience { get; init; }
}
