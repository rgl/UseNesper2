using System;
using Xunit.Sdk;

namespace UseNEsper2
{
    class Program
    {
        static void Main(string[] args)
        {
            // execute the tests with the poors' man runner...

            var tests = new EplTests();

            try
            {
                tests.GroupByRegression();
            }
            catch (XunitException e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
