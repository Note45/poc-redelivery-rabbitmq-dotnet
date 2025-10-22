using MassTransit;
using Microsoft.Extensions.Options;

namespace RedeliveryProject;

/// <summary> 
/// Consumer for the redelivery queue:
/// - Receives a message, waits for a configured delay, and resends it to the original queue, 
///   PRESERVING mt-redelivery-count and CorrelationId.
///   (Does not increment here.)
/// </summary> 
public class RedeliveryQueueConsumer : IConsumer<MyMessage>
{
    private readonly RedeliveryOptions _options;

    public RedeliveryQueueConsumer(IOptions<RedeliveryOptions> options)
    {
        _options = options.Value;
    }

    public async Task Consume(ConsumeContext<MyMessage> context)
    {
        int? count = MyMessageConsumer.TryGetRedeliveryCount(context);
        var cycleStr = count.HasValue ? count.Value.ToString() : "null";

        Console.WriteLine($"[redelivery-queue] Received for redelivery. Cycle={cycleStr}. " +
                          $"Waiting {_options.DelaySeconds}s...");

        // Wait for the configured delay (respecting cancellation)
        await Task.Delay(TimeSpan.FromSeconds(_options.DelaySeconds), context.CancellationToken);

        // Resend to the ORIGINAL queue (keeping the count as is)
        var mainQueue = await context.GetSendEndpoint(
            new Uri($"queue:{_options.MainQueue}")
        );
        await mainQueue.Send(new MyMessage(context.Message.Content), sendCtx =>
        {
            if (count.HasValue)
                sendCtx.Headers.Set("mt-redelivery-count", count.Value); // preserve the value

            // Preserve correlation
            sendCtx.CorrelationId = context.CorrelationId ?? Guid.NewGuid();
        });

        Console.WriteLine($"[redelivery-queue] Resent to {_options.MainQueue}. Cycle={cycleStr}. ACK.");
    }
}