using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Odbc;

namespace sap2redshift
{
    class redshift : IDisposable
    {
        private OdbcConnection conn;
        private DataTable schematab;

        public redshift(string server, string port, string db, string user, string pass)
        {

            string connString = "Driver={Amazon Redshift (x64)};" +
                                String.Format("Server={0};Database={1};" +
                                               "UID={2};PWD={3};Port={4};SSL=true;Sslmode=Require",
                                server, db, user, pass, port);

            // Make a connection using the psqlODBC provider.
            conn = new OdbcConnection(connString);
            conn.Open();
            Console.WriteLine("Connected to AWS Redshift " + DateTime.Now.ToString());
        }

        public DataTable executeQuery(string query)
        {

            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }

            OdbcCommand dc = new OdbcCommand(query, conn);
            OdbcDataReader reader = dc.ExecuteReader();

            schematab = reader.GetSchemaTable();

            DataTable dt = new DataTable();
            dt.Load(reader);

            reader.Close();

            return dt;
        }

        public int executeNonQuery(string query)
        {
            OdbcCommand dc = new OdbcCommand(query, conn);

            int rows = dc.ExecuteNonQuery();
            return rows;
        }
        public DataTable getLastSchema()
        {
            return schematab;
        }

        public void Dispose()
        {
            conn.Close();
            conn = null;
            schematab = null;
            Console.WriteLine("Disconnected from AWS Redshift " + DateTime.Now.ToString());
            GC.SuppressFinalize(this);
        }
    }
}
