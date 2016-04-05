using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORMTester
{
    using System.Collections;
    using System.Configuration;
    using System.Data;
    using System.Data.Entity;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Threading;

    using Dapper;

    using MathNet.Numerics.Statistics;

    class Program
    {
        static void Main(string[] args)
        {
            executionsPerSample = 100;
            var totalSamples = 100;
            var measurements = new Dictionary<string, IList<double>>(6)
                                   {
                                       {"Dapper Dynamic SQL with POCO Mapping",new List<double>()},
                                       {"Dapper Dynamic SQL", new List<double>() },
                                       {"Dapper Stored Procedure", new List<double>()},
                                       {"Dapper Stored Procedure with POCO Mapping",new List<double>()},
                                       {"Entity Framework Dynamic SQL",new List<double>()},
                                       {"Entity Framework SP", new List<double>() },
                                   };
            var delegates = new Dictionary<string, Action>(6)
                                {
                                       {"Dapper Dynamic SQL with POCO Mapping",RunDapperWithDynamicSqlToPoco},
                                       {"Dapper Dynamic SQL", RunDapperWithDynamicSql },
                                       {"Dapper Stored Procedure", RunEF},
                                       {"Dapper Stored Procedure with POCO Mapping",RunDapperDynamicSP},
                                       {"Entity Framework Dynamic SQL",RunDapperDynamicSP},
                                       {"Entity Framework SP", RunDapperPOCOSP },
                                };

            Console.WriteLine($"Running {totalSamples} samples of {executionsPerSample} queries per framework. Could take a minute.");
            for (int i = 0; i < totalSamples; i++)
            {
                foreach (var action in delegates)
                {
                    measurements[action.Key].Add(RunNTimes(action.Value, executionsPerSample));
                }
            }

            Console.WriteLine($"Results of {totalSamples} samples of {executionsPerSample} executions:");
            var results = (from measurement in measurements
                            let fiveNumberSummary = measurement.Value.FiveNumberSummary()
                            select
                                new Measurement()
                                    {
                                        Case = measurement.Key,
                                        Minimum = Math.Round(fiveNumberSummary[0]),
                                        LowerQuantile = Math.Round(fiveNumberSummary[1]),
                                        Median = Math.Round(fiveNumberSummary[2]),
                                        UpperQuantile = Math.Round(fiveNumberSummary[3]),
                                        Maximum = Math.Round(fiveNumberSummary[4])
                                    });

            Console.WriteLine("Case | Minimum | Lower Quantile | Median | Upper Quantile | Maximum");
            Console.WriteLine("--- | --- | --- | --- | --- | ---");
            foreach (var measurement in results)
            {
                Console.WriteLine(measurement.ToString());
            }

            Console.WriteLine("-----------------------------------");

            Console.Read();
        }

        public class Measurement
        {
            public string Case { get; set; }

            public double Minimum { get; set; }

            public double LowerQuantile { get; set; }

            public double Median { get; set; }

            public double UpperQuantile { get; set; }

            public double Maximum { get; set; }

            public override string ToString()
            {
                return $"{this.Case} | {this.Minimum} | {this.LowerQuantile} | {this.Median} | {this.UpperQuantile} | {this.Maximum}";
            }
        }

        private static void RunEF()
        {
            using (var context = new Model1())
            {
                var managerEmployeeses = context.Database.SqlQuery<ManagerEmployees>(sql, new SqlParameter("@BusinessEntityID", 2)).ToListAsync().Result;
            }
        }

        private static void RunEFSP()
        {
            using (var context = new Model1())
            {
                var managerEmployeeses = context.Database.SqlQuery<ManagerEmployees>("exec uspGetManagerEmployees @BusinessEntityID", new SqlParameter("@BusinessEntityID", 2)).ToListAsync().Result;
            }
        }

        private static long RunNTimes(Action action, int n)
        {
            var watch = Stopwatch.StartNew();
            for (int i = 0; i < n; i++)
            {
                action();
            }
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }

        private static void RunDapperWithDynamicSqlToPoco()
        {
            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
                )
            {
                connection.Open();
                connection.Query<ManagerEmployees>(sql, new { BusinessEntityId = 2 });
                connection.Close();
            }
        }

        private static void RunDapperDynamicSP()
        {
            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
                )
            {
                connection.Open();
                connection.Query("uspGetManagerEmployees", new {BusinessEntityId = 2}, commandType:CommandType.StoredProcedure);
                connection.Close();
            }
        }

        private static void RunDapperPOCOSP()
        {
            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
                )
            {
                connection.Open();
                connection.Query<ManagerEmployees>("uspGetManagerEmployees", new { BusinessEntityId = 2 }, commandType: CommandType.StoredProcedure);
                connection.Close();
            }
        }

        private static void RunDapperWithDynamicSql()
        {
            using (var connection = new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
                )
            {
                connection.Open();
                connection.Query(sql, new { BusinessEntityId = 2 });
                connection.Close();
            }
        }

        public class ManagerEmployees
        {
            public int RecursionLevel { get; set; }

            public string OrganizationNode { get; set; }

            public string ManagerFirstName { get; set; }

            public string ManagerLastName { get; set; }

            public int BusinessEntityID { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        static string sql = @"WITH [EMP_cte]([BusinessEntityID], [OrganizationNode], [FirstName], [LastName], [RecursionLevel]) -- CTE name and columns
    AS (
        SELECT e.[BusinessEntityID], e.[OrganizationNode], p.[FirstName], p.[LastName], 0 -- Get the initial list of Employees for Manager n
        FROM [HumanResources].[Employee] e 
			INNER JOIN [Person].[Person] p 
			ON p.[BusinessEntityID] = e.[BusinessEntityID]
        WHERE e.[BusinessEntityID] = @BusinessEntityID
        UNION ALL
        SELECT e.[BusinessEntityID], e.[OrganizationNode], p.[FirstName], p.[LastName], [RecursionLevel] + 1 -- Join recursive member to anchor
        FROM [HumanResources].[Employee] e 
            INNER JOIN [EMP_cte]
            ON e.[OrganizationNode].GetAncestor(1) = [EMP_cte].[OrganizationNode]
			INNER JOIN [Person].[Person] p 
			ON p.[BusinessEntityID] = e.[BusinessEntityID]
        )
    -- Join back to Employee to return the manager name 
    SELECT [EMP_cte].[RecursionLevel], [EMP_cte].[OrganizationNode].ToString() as [OrganizationNode], p.[FirstName] AS 'ManagerFirstName', p.[LastName] AS 'ManagerLastName',
        [EMP_cte].[BusinessEntityID], [EMP_cte].[FirstName], [EMP_cte].[LastName] -- Outer select from the CTE
    FROM [EMP_cte] 
        INNER JOIN [HumanResources].[Employee] e 
        ON [EMP_cte].[OrganizationNode].GetAncestor(1) = e.[OrganizationNode]
			INNER JOIN [Person].[Person] p 
			ON p.[BusinessEntityID] = e.[BusinessEntityID]
    ORDER BY [RecursionLevel], [EMP_cte].[OrganizationNode].ToString()
    OPTION (MAXRECURSION 25) ";

        private static int executionsPerSample;
    }
}
