using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Xml.Linq;

namespace SmartSqlDependencyNotification
{
    public class Program : IDisposable
    {
        #region Variable declaration

        private static string optionSelected;

        private readonly string sourceConnectionString;
        private readonly string destConnectionString;

        private readonly string notificationQuery;

        private readonly string notificationStoredProcedure;

        private SqlDependencyEx sqlDependency;

        /// <summary>
        /// SQL command.
        /// </summary>
        private SqlCommand sourceSqlCommand;

        /// <summary>
        /// SQL connection
        /// </summary>
        private SqlConnection sampleSqlConnection;

        private const string DATABASE_NAME = "SampleDB";

        private const string TABLE_NAME = "SampleTable01";

        #endregion

        # region Update Destination Db

        #endregion

        #region Constructor

        /// <summary>
        /// Prevents a default instance of the <see cref="Program"/> class from being created. 
        /// </summary>
        private Program()
        {
            this.sourceConnectionString = ConfigurationManager.ConnectionStrings["SourceDbConnection"].ConnectionString;
            this.destConnectionString = ConfigurationManager.ConnectionStrings["DestDbConnection"].ConnectionString;
            this.notificationQuery = "SELECT [SampleId],[SampleName],[SampleCategory],[SampleDateTime],[IsSampleProcessed] FROM [dbo].[SampleTable01];";
            this.notificationStoredProcedure = "uspGetSampleInformation";
        }

        #endregion

        #region Methods

        /// <summary>
        /// Main method.
        /// </summary>
        /// <param name="args">Input arguments.</param>
        public static void Main(string[] args)
        {
            var program = new Program();
            Console.WriteLine("Smartcare Sql Depedency Notification started...");
            program.Notification();
                    

            Console.ReadLine();
            program.Dispose();
        }

        /// <summary>
        /// Dispose all used resources.
        /// </summary>
        public void Dispose()
        {            
            if (null != this.sourceSqlCommand)
            {
                this.sourceSqlCommand.Dispose();
            }

            if (null != this.sampleSqlConnection)
            {
                this.sampleSqlConnection.Dispose();
            }

            if (null != this.sqlDependency)
            {
                this.sqlDependency.Dispose();
            }

            this.sourceSqlCommand = null;
            this.sampleSqlConnection = null;

            if (optionSelected.Equals("1") || optionSelected.Equals("3"))
            {
                SqlDependency.Stop(this.sourceConnectionString);
            }
            else if (optionSelected.Equals("2") || optionSelected.Equals("4"))
            {
                SqlDependency.Start(this.sourceConnectionString, "QueueSampleInformationDataChange");
            }
        }       

        private void Notification()
        {
            while (true)
            {
                using (this.sqlDependency = new SqlDependencyEx(
                            this.sourceConnectionString,
                            DATABASE_NAME,
                            TABLE_NAME, "dbo"))
                {
                    //sqlDependency.TableChanged += (o, e) => changesReceived++;    // original
                    sqlDependency.TableChanged += (o, e) =>
                    {
                        if (e.Data == null) return;

                        var insertedList = e.Data.Elements("inserted").Elements("row");//.Elements("SampleName");
                        //var sampleId;
                        //var sampleName;
                        //var sampleCategory;
                        //var sampleDateTime;
                        //var isSampleProcessed;

                        var deletedList = e.Data.Elements("deleted").Elements("row");
                        foreach (var j in deletedList)
                        {
                            string cmd = string.Format(@"UPDATE {0} Set IsCurrent=0 Where SampleId = {1} and IsCurrent=1;", TABLE_NAME, j.Element("SampleId").Value);
                            ExecuteNonQuery(cmd, this.destConnectionString);
                            Console.WriteLine(cmd);
                            //foreach (var i in j.Nodes())
                            //    Console.WriteLine("inserted: " + i);
                        }

                        foreach (var j in insertedList)
                        {
                            string cmd = string.Format(@"INSERT INTO {0} VALUES ({1}, '{2}', '{3}', '{4}', 1);", TABLE_NAME, j.Element("SampleId").Value, j.Element("SampleName").Value, j.Element("SampleCategory").Value, j.Element("SampleDateTime").Value);
                            ExecuteNonQuery(cmd, this.destConnectionString);
                            Console.WriteLine(cmd);
                            //foreach (var i in j.Nodes())
                            //    Console.WriteLine("inserted: " + i);
                        }


                        Console.WriteLine("\n");

                        //string.Format(SQL_FORMAT_INSERT, "SampleTable01",)

                        //var inserted = "(null)";
                        //if (e.Data.Element("inserted") != null) inserted = e.Data.Element("inserted").Element("row").Element("SampleName").ToString();
                        //var deleted = "(null)";
                        //if (e.Data.Element("deleted") != null) deleted = e.Data.Element("deleted").Element("row").Element("SampleName").ToString();
                        //Console.WriteLine("inserted: " + inserted);
                        //Console.WriteLine("deleted: " + deleted + "\n");

                        var wait = "temporary - just to debug above";
                    };
                    sqlDependency.Start();

                    // Wait a little bit to receive all changes.
                    Thread.Sleep(1000);
                }

                // Make sure we've released all resources.
                //Assert.AreEqual(changesCount, changesReceived); 
            }
        }

        private static void ExecuteNonQuery(string commandText, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(commandText, conn))
            {
                try
                {
                    conn.Open();
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    
                    throw;
                }
            }
        }

        #endregion
    }
}
