using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SAP.Middleware.Connector;

namespace sap2redshift
{
    class Program
    {
        static void Main(string[] args)
        {
            RfcConfigParameters ClientParams;
            RfcDestination destination;


            ClientParams = parameters.getSAPParams();

            try
            {
                destination = RfcDestinationManager.GetDestination(ClientParams);
                destination.Ping();

                //Console.WriteLine("dff");
                Console.WriteLine("Attributes (application server logon):");
                Console.WriteLine(destination.SystemAttributes);
                Console.WriteLine();
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.Source);
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine("Press Enter to exit");
                while (Console.ReadKey().Key != ConsoleKey.Enter)
                {
                }


            }

            try
            {
                RfcConfigParameters ServerParams = ClientParams;
                RfcServer my_server;

                

                //Dim server_mon As RfcServerMonitor
                Type[] myHandlers = new Type[1] { typeof(rfcserver) };
                String sKey;

                my_server = RfcServerManager.GetServer(ServerParams, myHandlers);


                my_server.Start();

                Console.WriteLine("NCO version " + SAPConnectorInfo.Version);
                Console.WriteLine("Server has been started. Press X to exit.\n");

                while (Console.ReadKey().Key != ConsoleKey.X) { }

                my_server.Shutdown(true);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException);
            }
        }
    }
}
