using MassTransit.Topology;

namespace RedeliveryProject;

public class CustomEntityNameFormatter : IEntityNameFormatter
{
    public string FormatEntityName<T>() => typeof(T).Name;
}