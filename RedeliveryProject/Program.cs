using GreenPipes;
using MassTransit;
using MassTransit.Topology;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace RedeliveryProject
{
    public record MyMessage(string Content);

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var host = Host.CreateDefaultBuilder(args) // load appsettings.json + env 
                .ConfigureServices((context, services) =>
                {
                    // Options: Redelivery 
                    services.Configure<RedeliveryOptions>(context.Configuration.GetSection("Redelivery"));

                    services.AddMassTransit(x =>
                    {
                        x.AddConsumer<MyMessageConsumer>();
                        x.AddConsumer<RedeliveryQueueConsumer>();

                        x.UsingRabbitMq((ctx, cfg) =>
                        {
                            var opts = ctx.GetRequiredService<IOptions<RedeliveryOptions>>().Value;

                            cfg.Host("localhost", "/", h => { });

                            // Main Exchange 
                            cfg.MessageTopology.SetEntityNameFormatter(new CustomEntityNameFormatter());

                            // Main Consumer config
                            cfg.ReceiveEndpoint(opts.MainQueue, e =>
                            {
                                e.Bind(opts.MainExchange, s =>
                                {
                                    s.RoutingKey = opts.MainRoutingKey;
                                    s.ExchangeType = "direct";
                                });

                                // Only redelevery logic has the retry logic
                                e.UseMessageRetry(r => r.None());

                                e.ConfigureConsumer<MyMessageConsumer>(ctx);
                            });

                            // Redelevery consumer logic 
                            cfg.ReceiveEndpoint(opts.RedeliveryQueue, e =>
                            {
                                // limit concurrency by options
                                var prefetch = (ushort)Math.Max(1, opts.RedeliveryPrefetch);
                                var concurrent = Math.Max(1, opts.RedeliveryConcurrentLimit);
                                e.PrefetchCount = prefetch;
                                e.ConcurrentMessageLimit = concurrent;

                                e.ConfigureConsumer<RedeliveryQueueConsumer>(ctx);
                            });
                        });
                    });

                    services.AddMassTransitHostedService();
                    services.AddSingleton<MessagePublisher>();
                })
                .Build();

            await host.StartAsync();

            var publisher = host.Services.GetRequiredService<MessagePublisher>();
            await publisher.SendMessage("Test (firt send without mt-redelivery count)");

            Console.WriteLine("Press any  key to exit...");
            Console.ReadKey();

            await host.StopAsync();
        }
    }

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


    // Options bind from appsettings.json 
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

    // Optional formatter for entity names 
    public class CustomEntityNameFormatter : IEntityNameFormatter
    {
        public string FormatEntityName<T>() => typeof(T).Name;
    }
}