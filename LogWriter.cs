using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MpesaService
{
	public class LogWriter
	{
		private string m_exePath = string.Empty;

		public LogWriter()
		{
			m_exePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
		}

		public void LogWrite(string logMessage, string logType)
		{

			try
			{
				using (StreamWriter w = File.AppendText("C:" + "\\" + "ProgramData" + "\\" + "SCL_SERVICE" + "\\" + "V1" + "\\" + "MPESASERVICE" + "\\" + logType + "-" + DateTime.Now.ToString("yyyyMMdd") + ".txt"))
				{
					Log(logMessage, w);
				}
			}
			catch (Exception ex)
			{
			}
		}

		public void Log(string logMessage, TextWriter txtWriter)
		{
			try
			{
				txtWriter.Write("\r\nLog Entry : ");
				txtWriter.WriteLine("{0} {1}", DateTime.Now.ToLongTimeString(),
				DateTime.Now.ToLongDateString());
				txtWriter.WriteLine("  -->");
				txtWriter.WriteLine("  :{0}", logMessage);
				txtWriter.WriteLine("================================================================================");
			}
			catch (Exception ex)
			{
			}
		}
	}
}

