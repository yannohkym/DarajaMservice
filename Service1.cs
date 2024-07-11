using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using static MpesaService.DataBaseHelper;

namespace MpesaService
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        Timer timer = new Timer();



        protected override void OnStart(string[] args)
        {
            WriteToFile("Transfer data Service is started at " + DateTime.Now);
            TransferData();
            timer.Elapsed += new ElapsedEventHandler(OnElapsedTime);
            timer.Interval = 5000; //number in milisecinds
            timer.Enabled = true;
        }
        protected override void OnStop()
        {
            TransferData();
            WriteToFile("Transfer data Service is stopped at " + DateTime.Now);
        }
        private void OnElapsedTime(object source, ElapsedEventArgs e)
        {
            WriteToFile("Transfer data Service is recalled at " + DateTime.Now);
        }
        public void WriteToFile(string Message)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + "\\Logs";
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            string filepath = AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\ServiceLog_" + DateTime.Now.Date.ToShortDateString().Replace('/', '_') + ".txt";
            if (!File.Exists(filepath))
            {
                // Create a file to write to.
                using (StreamWriter sw = File.CreateText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    sw.WriteLine(Message);
                }
            }
        }

        public void TransferData()
        {
            string sourceConnectionString = "Server=YANNOH; Database=ProductApi;User ID=sa;Password=Pass@123; TrustServerCertificate=true;";
            string destinationConnectionString = "Server=YANNOH; Database=IVEND;User ID=sa;Password=Pass@123; TrustServerCertificate=true;";
            string sourceTable = "Payments";
            string destinationTable = "Transactions";

			var columnMappings = new Dictionary<string, string>
            {
	        { "TransID", "U_TransID" },
	        { "TransactionType", "U_TransactionType" },
	        { "TransTime", "U_TransTime" },
	        { "TransAmount", "U_TransAmount" },
	        { "BusinessShortCode", "U_BusinessShortCode" },
	        { "BillRefNumber", "U_BillRefNumber" },
	        { "InvoiceNumber", "U_InvoiceNumber" },
	        { "OrgAccountBalance", "U_OrgAccountBalance" },
	        { "ThirdPartyTransID", "U_ThirdPartyTransID" },
	        { "MSISDN", "U_MSISDN" },
	        { "FirstName", "U_FirstName" },
	        { "MiddleName", "U_MiddleName" },
	        { "LastName", "U_LastName" },
	        { "status", "U_status" }
            };

			var dbHelper = new DataBaseHelper(sourceConnectionString, destinationConnectionString, columnMappings);
			dbHelper.TransferUniqueRecords(sourceTable, destinationTable);
		}
    }
}
