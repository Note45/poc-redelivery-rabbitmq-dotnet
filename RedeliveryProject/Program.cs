using GreenPipes;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace RedeliveryProject
{
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
}