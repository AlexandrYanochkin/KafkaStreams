using KafkaStreams.FourthTask.Models;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net;

namespace KafkaStreams.FourthTask.Services;

public class TopologyBuilder
{
    private Action<string, EmployeeInfo> resultHandler;

    public TopologyBuilder WithResultHandler(Action<string, EmployeeInfo> resultHandler)
    {
        this.resultHandler = resultHandler;
        return this;
    }

    public Topology Build(string topicName)
    {
        var builder = new StreamBuilder();

        builder.Stream<string, EmployeeInfo>(topicName)
            .Filter(Filter)
            .Foreach(ProcessResult);

        return builder.Build();
    }

    private bool Filter(string key, EmployeeInfo value)
    {
        return value is not null;
    }

    private void ProcessResult(string key, EmployeeInfo value)
    {
        resultHandler?.Invoke(key, value);
    }
}