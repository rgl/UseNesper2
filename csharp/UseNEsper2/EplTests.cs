using com.espertech.esper.client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Xunit;

namespace UseNEsper2
{
    public class ServiceEvent
    {
        public string Service { get; set; }
    }

    public class PropertiesEvent
    {
        public IDictionary<string, object> Properties { get; set; }
    }

    public class EplTests
    {
        [Fact]
        public void CastPropertyToIntRegression()
        {
            string expected;
            string resultProperties;
            var results = new List<EventBean[]>();

            using (var engine = CreateEngine())
            {
                engine.EPAdministrator.Configuration.AddEventType("PropertiesEvent", typeof(PropertiesEvent));

                var statement = engine.EPAdministrator.CreateEPL(@"
                    select cast(cast(Properties('value'), string), int) as Value
                    from PropertiesEvent
                ");

                resultProperties = GetStatementProperties(statement);

                expected = @"
                    Value:Int32

                    #0.0: Value:Int32=1
                ";

                statement.Events += (sender, args) => results.Add(args.NewEvents);

                engine.EPRuntime.SendEvent(
                    new PropertiesEvent
                    {
                        Properties = new Dictionary<string, object>()
                        {
                            {"value", "1"}
                        }
                    }
                );
            }

            var result = string.Format("{0}\n\n{1}", resultProperties, ResultsToString(results));

            Assert.Equal(FormatResult(expected), result);
        }

        [Fact]
        public void GroupByRegression()
        {
            string expected;
            string resultProperties;
            var results = new List<EventBean[]>();

            using (var engine = CreateEngine())
            {
                engine.EPAdministrator.Configuration.AddEventType("Service", typeof(ServiceEvent));

                var statement = engine.EPAdministrator.CreateEPL(@"
                    select Service, count(*) as Value
                    from Service.win:time_batch(1 seconds)
	                group by Service
                    order by Service
                ");

                resultProperties = GetStatementProperties(statement);

                expected = @"
                    Service:String
                    Value:Int64

                    #0.0: Service:(null)=(null) Value:Int64=3
                    #0.1: Service:String=Test Value:Int64=7

                    #1.0: Service:(null)=(null) Value:Int64=3
                    #1.1: Service:String=Test Value:Int64=7

                    #2.0: Service:(null)=(null) Value:Int64=0
                    #2.1: Service:String=Test Value:Int64=0
                ";

                statement.Events += (sender, args) => results.Add(args.NewEvents);

                for (var n = 0; n < 10; ++n)
                {
                    engine.EPRuntime.SendEvent(
                        new ServiceEvent
                        {
                            Service = n % 4 == 0 ? null : "Test"
                        }
                    );
                }

                Thread.Sleep(TimeSpan.FromSeconds(1.5));

                for (var n = 0; n < 10; ++n)
                {
                    engine.EPRuntime.SendEvent(
                        new ServiceEvent
                        {
                            Service = n % 4 == 0 ? null : "Test"
                        }
                    );
                }

                Thread.Sleep(TimeSpan.FromSeconds(2.5));
            }

            var result = string.Format("{0}\n\n{1}", resultProperties, ResultsToString(results));

            Assert.Equal(FormatResult(expected), result);
        }

        [Fact]
        public void FilterStreamAboveOrEqualWithMapEventType()
        {
            string expected;
            string resultProperties;
            var results = new List<EventBean[]>();

            using (var engine = CreateEngine())
            {
                engine.EPAdministrator.Configuration.AddEventType(
                    "ServiceHitEnd",
                    new Dictionary<string, object>()
                    {
                        { "HttpStatus", typeof(Int32) }
                    }
                );

                var statement = engine.EPAdministrator.CreateEPL(@"
                    select count(*) as Value
                    from ServiceHitEnd(HttpStatus >= 400).win:time_batch(1 seconds)
                ");

                resultProperties = GetStatementProperties(statement);

                expected = @"
                    Value:Int64

                    #0.0: Value:Int64=2
                    
                    #1.0: Value:Int64=2
                    
                    #2.0: Value:Int64=0
                ";

                statement.Events += (sender, args) => results.Add(args.NewEvents);

                for (var n = 200; n < 600; n += 100)
                {
                    engine.EPRuntime.SendEvent(
                        new Dictionary<string, object>()
                        {
                            { "HttpStatus", n }
                        },
                        "ServiceHitEnd"
                    );
                }

                Thread.Sleep(TimeSpan.FromSeconds(1.5));

                for (var n = 200; n < 600; n += 100)
                {
                    engine.EPRuntime.SendEvent(
                        new Dictionary<string, object>()
                        {
                            { "HttpStatus", n }
                        },
                        "ServiceHitEnd"
                    );
                }

                Thread.Sleep(TimeSpan.FromSeconds(2.5));
            }

            var result = string.Format("{0}\n\n{1}", resultProperties, ResultsToString(results));

            Assert.Equal(FormatResult(expected), result);
        }

        private static EPServiceProvider CreateEngine()
        {
            var configuration = new Configuration();

            configuration.AddEventType<ServiceEvent>("Service");

            var engine = EPServiceProviderManager.GetProvider("Engine", configuration);

            return engine;
        }

        private static string GetStatementProperties(EPStatement statement)
        {
            return string.Join(
                "\n",
                statement
                    .EventType
                    .PropertyDescriptors
                    .Select(d => string.Format("{0}:{1}", d.PropertyName, GetStatementPropertyType(d.PropertyName, d.PropertyType)))
            );
        }


        public static string GetStatementPropertyType(string propertyName, Type propertyType)
        {
            if (propertyType.IsGenericType)
            {
                // if it's a Nullable we really want to get the type of the type wrapped by the nullable.
                var underlyingType = Nullable.GetUnderlyingType(propertyType);

                if (underlyingType != null)
                {
                    return GetPropertyType(underlyingType);
                }

                // if it's a ICollection (or its base interface IEnumerable) return it as Array
                if (propertyType.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
                {
                    return "Array";
                }

                throw new ArgumentOutOfRangeException(string.Format("the property {0} has the unsupported property type {1}", propertyName, propertyType));
            }
            else
            {
                return GetPropertyType(propertyType);
            }
        }

        private static string GetPropertyType(Type type)
        {
            if (type.IsEnum)
                return "String";

            if (type.IsArray)
                return "Array";

            var typeName = type.Name;

            switch (typeName)
            {
                case "Boolean":
                case "DateTime":
                case "Double":
                case "Int32":
                case "Int64":
                case "String":
                    return typeName;

                default:
                    return "Object";
            }
        }

        private static string FormatResult(string s)
        {
            return Regex.Replace(Regex.Replace(s, @"^[ \t]+", "", RegexOptions.Multiline).Trim(), @"\r", "");
        }

        private static string ResultsToString(IEnumerable<EventBean[]> results)
        {
            return string.Join("\n\n", results.Select(ResultToString));
        }

        private static string ResultToString(EventBean[] result, int n)
        {
            return string.Join("\n", result.Select((r, i) => ResultToString(r, i, n)));
        }

        private static string ResultToString(EventBean result, int i, int n)
        {
            return string.Format(
                "#{0}.{1}: {2}",
                n,
                i,
                string.Join(
                    " ",
                    result.EventType.PropertyNames
                        .Select(
                            name =>
                            {
                                var value = result.Get(name);

                                return string.Format("{0}:{1}={2}", name, value != null ? value.GetType().Name : "(null)", value ?? "(null)");
                            }
                        )
                )
            );
        }
    }
}
