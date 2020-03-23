using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLDataToCSVConverter
{
    class Program
    {
        static readonly string DistinctColumnName = ConfigurationManager.AppSettings["DistinctColumnName"];
        static void Main(string[] args)
        {
            ServiceProvider.LogDetails("Service Started.");
            GenerateCSV();
            ServiceProvider.LogDetails("Service Ended.");
        }
        static void GenerateCSV()
        {
            var columnName = DistinctColumnName;//"ApplicationObjectTypeName";
            //"AJF_PROJ_CDE (C13)"
            var table = ServiceProvider.ReadTable();
            //AJF_PROJ_CDE (C13)
            var projects = table.AsEnumerable().Select(row => row.Field<string>(columnName)).Distinct();//Select(group => group.First()).ToList();
            foreach (var project in projects)
            {
                DataTable selectedTableRecords = table.AsEnumerable()
                            .Where(r => r.Field<string>(columnName) == project)
                            .CopyToDataTable();

                ServiceProvider.LogDetails("Data Table retrieved with " + selectedTableRecords.Rows.Count + ".");
                string exe = Process.GetCurrentProcess().MainModule.FileName;
                string filepath = Path.GetDirectoryName(exe) + @"\Files\Report_" + project+"_" + DateTime.Now.ToString("yyyyMMddHHmm") + ".csv";
                ServiceProvider.WriteToFile(selectedTableRecords, filepath, false, ",");
                ServiceProvider.LogDetails("File Generated at " + filepath);
            }

        }
    }
}
