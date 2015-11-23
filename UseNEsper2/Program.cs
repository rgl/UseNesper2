using com.espertech.esper.client;
using System;
using System.Text;
using System.Threading;

namespace UseNEsper2
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = new Configuration();

            //configuration.AddPlugInSingleRowFunction("current_time", typeof(Time).FullName, "Current");

            configuration.AddEventType<ServiceEvent>("Service");

            var engine = EPServiceProviderManager.GetProvider("Engine", configuration);

            var allStatement = StartStatement(
                engine,
                "All",
                @"
                    select
                        //current_time() as Time,
                        Service,
                        Service is null as ServiceIsNull
                    from
                        Service.win:time_batch(5 seconds)
                "
            );

            var groupsStatement = StartStatement(
                engine,
                "Groups",
                @"
                    select
                        //current_time() as Time,
                        Service,
                        Service is null as IsServiceNull,
                        count(*) as Services,
                        count(Service is null) as NullServices,
                        count(Service is not null) as NotNullServices
                    from
                        Service.win:time_batch(5 seconds)
                    group by
                        Service
                "
            );

            var eventSender = engine.EPRuntime.GetEventSender("Service");

            for (var n = 0; !Console.KeyAvailable; ++n)
            {
                var serviceEvent = new ServiceEvent
                {
                    Service = n % 2 == 0 ? "Service"+n : null,
                };

                eventSender.SendEvent(serviceEvent);

                if (n%10 == 0)
                {
                    Console.WriteLine("{0} Sent {1} events", DateTime.Now, n);
                }

                Thread.Sleep(500);
            }
        }

        private static EPStatement StartStatement(EPServiceProvider engine, string statementName, string epl)
        {
            var statement = engine.EPAdministrator.CreateEPL(epl);

            statement.Events += (sender, e) =>
            {
                var result = new StringBuilder();
                result.AppendFormat("{0} {1} Batch:\n", DateTime.Now, statementName);
                DumpEvents("OldEvents", e.OldEvents, result);
                DumpEvents("NewEvents", e.NewEvents, result);
                Console.WriteLine(result.ToString());
            };

            return statement;
        }

        private static void DumpEvents(string title, EventBean[] e, StringBuilder result)
        {
            if (e == null)
                return;

            result.AppendFormat("  {0}:\n", title);

            foreach (var bean in e)
            {
                foreach (var name in bean.EventType.PropertyNames)
                {
                    result.AppendFormat("    {0}={1}", name, bean.Get(name) ?? "(null)");
                }

                result.AppendLine();
            }
        }
    }

    public class Time
    {
        public static DateTime Current()
        {
            return DateTime.UtcNow;
        }
    }

    public class ServiceEvent
    {
        public string Service { get; set; }
    }
}
