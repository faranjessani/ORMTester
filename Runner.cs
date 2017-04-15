using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using Dapper;
using MathNet.Numerics.Statistics;

namespace ORMTester
{
    internal class Runner
    {
        public static int ExecutionsPerSample { get; private set; }

        public void Run()
        {
            ExecutionsPerSample = 100;
            var totalSamples = 100;
            var testCases = new List<Tuple<string, Action>>
            {
                new Tuple<string, Action>(
                    "Dapper Stored Procedure",
                    RunDapperDynamicSP),
                new Tuple<string, Action>(
                    "Dapper Stored Procedure with POCO Mapping",
                    RunDapperPOCOSP),
                new Tuple<string, Action>(
                    "Entity Framework Dynamic SQL",
                    RunEF),
                new Tuple<string, Action>(
                    "Entity Framework SP",
                    RunEFSP),
                new Tuple<string, Action>(
                    "SQLDataReader SP By Row",
                    () => RunSqlDataReaderByRow("exec uspGetManagerEmployees @BusinessEntityID")),
                new Tuple<string, Action>(
                    "SQLDataReader Dynamic SQL By Row",
                    () => RunSqlDataReaderByRow(sql)),
                new Tuple<string, Action>(
                    "SQLDataReader Dynamic SQL Into DataSet",
                    () => RunSqlDataReaderIntoDataSet(sql)),
                new Tuple<string, Action>(
                    "SQLDataReader SP Into DataSet",
                    () => RunSqlDataReaderIntoDataSet("exec uspGetManagerEmployees @BusinessEntityID")),
                new Tuple<string, Action>(
                    "Dapper Dynamic SQL",
                    RunDapperWithDynamicSql),
                new Tuple<string, Action>(
                    "Dapper Dynamic SQL with POCO Mapping",
                    RunDapperWithDynamicSqlToPoco)
            };
            var measurements =
                testCases.ToDictionary<Tuple<string, Action>, string, IList<double>>(testCase => testCase.Item1,
                    testCase => new List<double>());

            Console.WriteLine(
                $"Running {totalSamples} samples of {ExecutionsPerSample} queries per framework. Could take a minute.");
            for (var i = 0; i < totalSamples; i++)
                foreach (var action in testCases)
                    measurements[action.Item1].Add(RunNTimes(action.Item2, ExecutionsPerSample));

            Console.WriteLine($"Results of {totalSamples} samples of {ExecutionsPerSample} executions:");
            var results = from measurement in measurements
                let fiveNumberSummary = measurement.Value.FiveNumberSummary()
                orderby fiveNumberSummary.Median()
                select
                new Measurement
                {
                    Case = measurement.Key,
                    Minimum = Math.Round(fiveNumberSummary[0]),
                    LowerQuantile = Math.Round(fiveNumberSummary[1]),
                    Median = Math.Round(fiveNumberSummary[2]),
                    UpperQuantile = Math.Round(fiveNumberSummary[3]),
                    Maximum = Math.Round(fiveNumberSummary[4])
                };

            Console.WriteLine("Case | Minimum | Lower Quantile | Median | Upper Quantile | Maximum");
            Console.WriteLine("--- | --- | --- | --- | --- | ---");
            foreach (var measurement in results)
                Console.WriteLine(measurement.ToString());

            Console.WriteLine("-----------------------------------");
        }

        private static readonly string sql =
            @"WITH [EMP_cte]([BusinessEntityID], [OrganizationNode], [FirstName], [LastName], [RecursionLevel]) -- CTE name and columns
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

        private static void ClearDbCache()
        {
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                using (var command = new SqlCommand("DBCC FREEPROCCACHE;DBCC DROPCLEANBUFFERS;", connection))
                {
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        private static void RunSqlDataReaderByRow(string sql)
        {
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add(new SqlParameter("@BusinessEntityID", 2));
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var employees = new ManagerEmployees
                        {
                            BusinessEntityID = reader.GetInt32(4),
                            FirstName = reader.GetString(5),
                            LastName = reader.GetString(6),
                            ManagerFirstName = reader.GetString(2),
                            ManagerLastName = reader.GetString(3),
                            OrganizationNode = reader.GetString(1),
                            RecursionLevel = reader.GetInt32(0)
                        };
                    }
                    reader.Close();
                }
                connection.Close();
            }
        }

        private static void RunSqlDataReaderIntoDataSet(string sql)
        {
            var dataTable = new DataTable();
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.Add(new SqlParameter("@BusinessEntityID", 2));
                    using (var da = new SqlDataAdapter(command))
                    {
                        da.Fill(dataTable);
                    }
                }
                connection.Close();
            }

            for (var i = 0; i < dataTable.Rows.Count; i++)
            {
                var employees = new ManagerEmployees
                {
                    BusinessEntityID = (int) dataTable.Rows[i]["BusinessEntityId"],
                    FirstName = (string) dataTable.Rows[i]["FirstName"],
                    LastName = (string) dataTable.Rows[i]["LastName"],
                    ManagerFirstName = (string) dataTable.Rows[i]["ManagerFirstName"],
                    ManagerLastName = (string) dataTable.Rows[i]["ManagerLastName"],
                    OrganizationNode = (string) dataTable.Rows[i]["OrganizationNode"],
                    RecursionLevel = (int) dataTable.Rows[i]["RecursionLevel"]
                };
            }
        }

        private static void RunEF()
        {
            using (var context = new Model1())
            {
                var managerEmployeeses =
                    context.Database.SqlQuery<ManagerEmployees>(sql, new SqlParameter("@BusinessEntityID", 2))
                        .ToListAsync()
                        .Result;
            }
        }

        private static void RunEFSP()
        {
            using (var context = new Model1())
            {
                var managerEmployeeses =
                    context.Database.SqlQuery<ManagerEmployees>("exec uspGetManagerEmployees @BusinessEntityID",
                        new SqlParameter("@BusinessEntityID", 2)).ToListAsync().Result;
            }
        }

        private static long RunNTimes(Action action, int n)
        {
            ClearDbCache();
            var watch = Stopwatch.StartNew();
            for (var i = 0; i < n; i++)
                action();
            watch.Stop();
            return watch.ElapsedMilliseconds;
        }

        private static void RunDapperWithDynamicSqlToPoco()
        {
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                connection.Query<ManagerEmployees>(sql, new {BusinessEntityId = 2});
                connection.Close();
            }
        }

        private static void RunDapperDynamicSP()
        {
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                connection.Query("uspGetManagerEmployees", new {BusinessEntityId = 2},
                    commandType: CommandType.StoredProcedure);
                connection.Close();
            }
        }

        private static void RunDapperPOCOSP()
        {
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                connection.Query<ManagerEmployees>("uspGetManagerEmployees", new {BusinessEntityId = 2},
                    commandType: CommandType.StoredProcedure);
                connection.Close();
            }
        }

        private static void RunDapperWithDynamicSql()
        {
            using (
                var connection =
                    new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks"].ConnectionString)
            )
            {
                connection.Open();
                connection.Query(sql, new {BusinessEntityId = 2});
                connection.Close();
            }
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
                return $"{Case} | {Minimum} | {LowerQuantile} | {Median} | {UpperQuantile} | {Maximum}";
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
    }
}