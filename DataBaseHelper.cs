		using System;
		using System.Collections.Generic;
		using System.Data.SqlClient;
		using System.Data;
		using System.Linq;
		using System.Text;
		using System.Threading.Tasks;
using System.Diagnostics;



		namespace MpesaService
		{
			public class DataBaseHelper
			{
				private string sourceConnectionString;
				private string destinationConnectionString;
				private Dictionary<string, string> columnMappings;
				LogWriter logWriter = new LogWriter();

				public DataBaseHelper(string sourceConnectionString, string destinationConnectionString, Dictionary<string, string> columnMappings)
				{
					this.sourceConnectionString = sourceConnectionString;
					this.destinationConnectionString = destinationConnectionString;
					this.columnMappings = columnMappings;
				}

				public void TransferUniqueRecords(string sourceTable, string destinationTable)
				{
					DataTable dataTable = new DataTable();

					try
					{
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
									string columns = string.Join(", ", columnMappings.Values);
									string values = string.Join(", ", columnMappings.Values.Select(col => "@" + col));

									string insertCommandText = $"INSERT INTO {destinationTable} ({columns}) VALUES ({values})";

									using (SqlCommand insertCommand = new SqlCommand(insertCommandText, destinationConnection))
									{
										foreach (var mapping in columnMappings)
										{
											insertCommand.Parameters.AddWithValue("@" + mapping.Value, row[mapping.Key]);
										}

											insertCommand.ExecuteNonQuery();
									}
								}
							}
						}
					}
					catch (SqlException ex)
					{
						Console.WriteLine($"SQL Error: {ex.Message}");
				        logWriter.LogWrite($"SQL Error: {ex.Message}", "SQL ERROR");
						logWriter.LogWrite($"SQL Error: {ex.StackTrace}", "SQL ERROR0");

						Console.WriteLine($"Stack Trace: {ex.StackTrace}");
						throw;  // Rethrow the exception to terminate the process or handle it at a higher level
					}
					catch (Exception ex)
					{
						Console.WriteLine($"General Error: {ex.Message}");
						logWriter.LogWrite($"SQL Error: {ex.Message}", "SQL ERROR1");
						logWriter.LogWrite($"SQL Error: {ex.StackTrace}", "SQL ERROR2");
						Console.WriteLine($"Stack Trace: {ex.StackTrace}");
						throw;  // Rethrow the exception to terminate the process or handle it at a higher level
					}
				}

				private bool RecordExists(SqlConnection connection, string destinationTable, DataRow row)
				{
					// Assuming U_TransID is the unique key for simplicity
					string transID = row["TransID"].ToString();
					string checkCommandText = $"SELECT COUNT(1) FROM {destinationTable} WHERE U_TransID = @U_TransID";

					using (SqlCommand checkCommand = new SqlCommand(checkCommandText, connection))
					{
						checkCommand.Parameters.AddWithValue("@U_TransID", transID);
						return (int)checkCommand.ExecuteScalar() > 0;
					}
				}
			}
		}







