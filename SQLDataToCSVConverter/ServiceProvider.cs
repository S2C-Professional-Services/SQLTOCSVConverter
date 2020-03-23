using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataToCSVConverter
{
    public static class ServiceProvider
    {
        // Simple example for 
        // 1.) Read a sql server query to datatable;
        // 2.) Export it to .csv
        private static readonly string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static readonly string TextDelimiter = "\"";
        //private static readonly string SQLQuery = "Select * from Users";
        public static DataTable ReadTable()
        {
            var returnValue = new DataTable();

            var conn = new SqlConnection(ConnectionString);

            try
            {

                var sqlQuery = ReadSQLQuery();
                if (!string.IsNullOrEmpty(sqlQuery))
                {
                    conn.Open();
                    var command = new SqlCommand(sqlQuery, conn);

                    using (var adapter = new SqlDataAdapter(command))
                    {
                        adapter.Fill(returnValue);
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                LogDetails(ex.Message + " INNER EXCEPTIONS: " + ex.InnerException);
                //throw ex;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();
            }

            return returnValue;
        }

        public static void WriteToFile(DataTable dataSource, string fileOutputPath, bool firstRowIsColumnHeader = false, string seperator = ";")
        {
            try
            {
                var sw = new StreamWriter(fileOutputPath, false);

                int icolcount = dataSource.Columns.Count;

                if (!firstRowIsColumnHeader)
                {
                    for (int i = 0; i < icolcount; i++)
                    {
                        sw.Write(TextDelimiter + dataSource.Columns[i]+ TextDelimiter);
                        if (i < icolcount - 1)
                            sw.Write(seperator);
                    }

                    sw.Write(sw.NewLine);
                }

                foreach (DataRow drow in dataSource.Rows)
                {
                    for (int i = 0; i < icolcount; i++)
                    {
                        if (!Convert.IsDBNull(drow[i]))
                            sw.Write(TextDelimiter + drow[i].ToString()+ TextDelimiter);
                        if (i < icolcount - 1)
                            sw.Write(seperator);
                    }
                    sw.Write(sw.NewLine);
                }
                sw.Close();
            }
            catch (Exception ex)
            {
                LogDetails(ex.Message + " INNER EXCEPTIONS: " + ex.InnerException);
            }
        }
        public static void LogDetails(string content)
        {
            string exe = Process.GetCurrentProcess().MainModule.FileName;
            string path = Path.GetDirectoryName(exe);
            FileStream fs = new FileStream(path + @"\Logs" + @"\ServiceLog_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".txt", FileMode.OpenOrCreate, FileAccess.Write);
            StreamWriter sw = new StreamWriter(fs);
            sw.BaseStream.Seek(0, SeekOrigin.End);
            sw.WriteLine(content + " [At " + DateTime.Now.ToString("yyyyMMddHHmmss") + "]" + Environment.NewLine);
            sw.Flush();
            sw.Close();
        }

        private static string ReadSQLQuery()
        {
            string exe = Process.GetCurrentProcess().MainModule.FileName;
            string path = Path.GetDirectoryName(exe);
            return File.ReadAllText(path + @"\SQLQuery\SQLScript.sql");
        }
    }
}
