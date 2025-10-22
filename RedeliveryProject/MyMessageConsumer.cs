using MassTransit;
using Microsoft.Extensions.Options;

namespace RedeliveryProject;

/// <summary> 
/// Consumer for the main queue. 
/// - In case of a recoverable failure, sends to the 'redelivery-queue':
///   - if the header is null, sets mt-redelivery-count = 1 (first redelivery);
///   - if it has value N, sets mt-redelivery-count = N + 1.
/// - When MaxRedeliveryCycles is exceeded, throws an exception to send the message to *_error.
/// </summary> 
public class MyMessageConsumer : IConsumer<MyMessage>
{
    private const string RedeliveryHeader = "mt-redelivery-count";
    private readonly RedeliveryOptions _options;

    public MyMessageConsumer(IOptions<RedeliveryOptions> options)
    {
        _options = options.Value;
    }

    public async Task Consume(ConsumeContext<MyMessage> context)
    {
        int? count = TryGetRedeliveryCount(context);
        var cycleStr = count.HasValue ? count.Value.ToString() : "null";
        Console.WriteLine($"[main-queue] Received: \"{context.Message.Content}\" | Cycle={cycleStr}");

        // Simulate a business failure
        var successfulDelivery = false;

        if (successfulDelivery)
        {
            Console.WriteLine("Successfully processed (ACK).");
            return; // ACK
        }

        var currentCount = count ?? 0; // null -> 0 (first pass)
        if (currentCount < _options.MaxRedeliveryCycles)
        {
            // Next value: if header is null, becomes 1; otherwise, increments
            var nextCount = count.HasValue ? count.Value + 1 : 1;

            // Send to the redelivery queue (consumer will wait for Delay and resend to main)
            var redelivery = await context.GetSendEndpoint(
                new Uri($"queue:{_options.RedeliveryQueue}")
            );
            await redelivery.Send(new MyMessage(context.Message.Content),
                sendCtx =>
                {
                    sendCtx.Headers.Set(RedeliveryHeader, nextCount); // <<<<<< set to 1 if it was null
                    sendCtx.CorrelationId = context.CorrelationId ?? Guid.NewGuid();
                });

            Console.WriteLine(
                count.HasValue
                    ? $"Scheduled next cycle #{nextCount} (via redelivery consumer)."
                    : "First redelivery detected (null header). Setting mt-redelivery-count=1 and scheduling."
            );
            return; // ACK
        }

        Console.WriteLine("Retry limit exceeded. Sending to _error.");
        throw new Exception("Failure after all redelivery cycles.");
    }

    internal static int? TryGetRedeliveryCount(ConsumeContext context)
    {
        const string RedeliveryHeader = "mt-redelivery-count";
        if (!context.Headers.TryGetHeader(RedeliveryHeader, out var value) || value == null)
            return null;

        if (value is int i) return i;
        if (int.TryParse(value.ToString(), out var parsed)) return parsed;
        return null;
    }
}