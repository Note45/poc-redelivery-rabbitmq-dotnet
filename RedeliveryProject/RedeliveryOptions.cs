namespace RedeliveryProject;

public class RedeliveryOptions
{
    public int DelaySeconds { get; set; } = 10;
    public int MaxRedeliveryCycles { get; set; } = 3;

    public string MainQueue { get; set; } = "main-queue";
    public string MainExchange { get; set; } = "main-exchange";
    public string MainRoutingKey { get; set; } = "main-key";

    public string RedeliveryQueue { get; set; } = "redelivery-queue";

    public int RedeliveryPrefetch { get; set; } = 1;
    public int RedeliveryConcurrentLimit { get; set; } = 1;
}