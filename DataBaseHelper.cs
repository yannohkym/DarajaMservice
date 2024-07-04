using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MpesaService
{
  
  

    public class DataBaseHelper
    {
        private string sourceConnectionString;
        private string destinationConnectionString;

        public DataBaseHelper(string sourceConnectionString, string destinationConnectionString)
        {
            this.sourceConnectionString = sourceConnectionString;
            this.destinationConnectionString = destinationConnectionString;
        }

        public void TransferUniqueRecords(string sourceTable, string destinationTable)
        {
            DataTable dataTable = new DataTable();

            using (SqlConnection sourceConnection = new SqlConnection(sourceConnectionString))
            {
                sourceConnection.Open();

                // Read records from source table
                using (SqlCommand readCommand = new SqlCommand($"SELECT * FROM {sourceTable}", sourceConnection))
                using (SqlDataAdapter adapter = new SqlDataAdapter(readCommand))
                {
                    adapter.Fill(dataTable);
                }
            }

            using (SqlConnection destinationConnection = new SqlConnection(destinationConnectionString))
            {
                destinationConnection.Open();

                // Insert unique records into destination table
                foreach (DataRow row in dataTable.Rows)
                {
                    if (!RecordExists(destinationConnection, destinationTable, row))
                    {
                        string columns = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(col => col.ColumnName));
                        string values = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(col => "@" + col.ColumnName));

                        string insertCommandText = $"INSERT INTO {destinationTable} ({columns}) VALUES ({values})";

                        using (SqlCommand insertCommand = new SqlCommand(insertCommandText, destinationConnection))
                        {
                            foreach (DataColumn column in dataTable.Columns)
                            {
                                insertCommand.Parameters.AddWithValue("@" + column.ColumnName, row[column]);
                            }

                            insertCommand.ExecuteNonQuery();
                        }
                    }
                }
            }
        }

        private bool RecordExists(SqlConnection connection, string destinationTable, DataRow row)
        {
            // Assuming Id is the unique key for simplicity
            int id = (int)row["Id"];
            string checkCommandText = $"SELECT COUNT(1) FROM {destinationTable} WHERE Id = @Id";

            using (SqlCommand checkCommand = new SqlCommand(checkCommandText, connection))
            {
                checkCommand.Parameters.AddWithValue("@Id", id);
                return (int)checkCommand.ExecuteScalar() > 0;
            }
        }
    }



}

