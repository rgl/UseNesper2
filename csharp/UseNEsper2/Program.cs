using System;

namespace UseNEsper2
{
    class Program
    {
        static void Main(string[] args)
        {
            // execute the tests with the poors' man runner...
            RunTest("CastPropertyToIntRegression", new EplTests().CastPropertyToIntRegression);
            RunTest("GroupByRegression", new EplTests().GroupByRegression);
            RunTest("FilterStreamAboveOrEqualWithMapEventType", new EplTests().FilterStreamAboveOrEqualWithMapEventType);
        }

        private static void RunTest(string title, Action test)
        {
            try
            {
                Console.WriteLine("RUN  {0}", title);
                test();
                Console.WriteLine("OKAY {0}", title);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("FAIL {0}: {1}\n\n", title, e);
            }
        }
    }
}
