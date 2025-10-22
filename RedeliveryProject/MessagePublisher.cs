using MassTransit;
using Microsoft.Extensions.Options;

namespace RedeliveryProject;

public class MessagePublisher
{
    private readonly ISendEndpointProvider _send;
    private readonly RedeliveryOptions _options;

    public MessagePublisher(ISendEndpointProvider send,
        IOptions<RedeliveryOptions> options)
    {
        _send = send;
        _options = options.Value;
    }

    public async Task SendMessage(string content)
    {
        var endpoint = await _send.GetSendEndpoint(
            new Uri($"queue:{_options.MainQueue}")
        );
        await endpoint.Send(new MyMessage(content), ctx =>
        {
            // >>> Do NOT set mt-redelivery-count on the first send <<<
            ctx.CorrelationId = Guid.NewGuid();
        });

        Console.WriteLine($"Message sent to {_options.MainQueue} (without mt-redelivery-count).");
    }
}