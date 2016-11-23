using System;
using System.Data;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using System.Text;
using System.Threading.Tasks;
using SAP.Middleware.Connector;

namespace sap2redshift
{
    class RSHParams : Dictionary<string, string>
    {

    }

    class parameters

    {
        public static RfcConfigParameters getSAPParams()
        {
            XDocument oSettingsXML;
            try
            {
                oSettingsXML = XDocument.Load("settings.xml");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }

            XElement oXMLRootElem = oSettingsXML.Element("SETTINGS").Element("SAP");

            RfcConfigParameters sapconfig = new RfcConfigParameters();

            foreach (XElement xe in oXMLRootElem.Elements())
            {
                sapconfig.Add(xe.Name.LocalName, xe.Value);
            };

            return sapconfig;
        }

        //Get Redshift parameters
        public static RSHParams getRSHParams()
        {
            XDocument oSettingsXML;
            try
            {
                oSettingsXML = XDocument.Load("settings.xml");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }

            XElement oXMLRootElem = oSettingsXML.Element("SETTINGS").Element("REDSHIFT");

            RSHParams rshparams = new RSHParams();

            foreach (XElement xe in oXMLRootElem.Elements())
            {
                rshparams.Add(xe.Name.LocalName, xe.Value);

            };

            return rshparams;
        }
    }
    public class rfcserver
    {
        static RSHParams rshparameters;
        static redshift rsh;

        static rfcserver()
        {
            rshparameters = parameters.getRSHParams();

            if (rshparameters["SERVER"] == "1")
            {
                getRedshift();
            }
        }

        [RfcServerFunction(Name = "ZAWS_RSH_EXEC_QUERY")]
        public static void exec_sql(RfcServerContext context, IRfcFunction function)
        {
            getRedshift();

            char bNonQuery = function.GetChar("IV_NONQUERY");
            string sSQL = function.GetString("IV_SQL");


            if (bNonQuery == 'X')
            {
                int rows = rsh.executeNonQuery(sSQL);
                function.SetValue("EV_STATUSCODE", rows.ToString());
            }
            else
            {
                DataTable tab = rsh.executeQuery(sSQL);
                function.SetValue("EV_STATUSCODE", tab.Rows.Count.ToString());

                IRfcTable tMetadata = function.GetTable("ET_METADATA");
                get_tableMetadata(rsh.getLastSchema(), ref tMetadata);

                function.SetValue("EV_DATA", convertData(tab));

            }

            if (rshparameters["SERVER"] == "0")
            {
                rsh.Dispose();
                rsh = null;
            }
        }

        private static void getRedshift()
        {
            if (rsh == null)
            {
                rsh = new redshift(rshparameters["SERVER"], rshparameters["PORT"], rshparameters["DB"], rshparameters["USER"], rshparameters["PASSWORD"]);
            }
        }
        private static void get_tableMetadata(DataTable tab, ref IRfcTable tMetadata)
        {
            for (int i = 0; i < tab.Rows.Count; i++)
            {

                tMetadata.Append();
                tMetadata.SetValue("FIELDNAME", tab.Rows[i]["ColumnName"]);
                tMetadata.SetValue("FIELDDESCR", tab.Rows[i]["ColumnName"]);

                Type dtype = (Type)tab.Rows[i]["DataType"];
                tMetadata.SetValue("TYPE", dtype.Name);

                tMetadata.SetValue("LENGTH", tab.Rows[i]["NumericPrecision"]);
                tMetadata.SetValue("DECIMALS", tab.Rows[i]["NumericScale"]);
            }

        }
        private static byte[] convertData(DataTable tab)
        {

            DataColumnCollection columns = tab.Columns;
            dataexport export = new dataexport();

            foreach (DataRow row in tab.Rows)
            {
                export.addRow();

                foreach (DataColumn col in columns)
                {

                    export.addField(col.ColumnName.ToUpper(), row[col.ColumnName]);
                }
            }

            return export.save();
        }
    }
    class dataexport
    {
        private XDocument xmldoc;
        private XElement eTable;
        private XElement item; //current position

        public dataexport()
        {

            XNamespace asxns = "http://www.sap.com/abapxml";

            xmldoc = new XDocument();
            eTable = new XElement("TABLE");

            xmldoc.Add(new XElement(asxns + "abap",
                new XAttribute(XNamespace.Xmlns + "asx", "http://www.sap.com/abapxml"),
                new XElement(asxns + "values", eTable)));
        }

        public void addField(XName FieldName, object Value)
        {
            if (item == null) return;
            item.Add(new XElement(FieldName, Value));
        }
        public void addRow()
        {

            item = new XElement("item");
            eTable.Add(item);
        }
        public byte[] save()
        {
            MemoryStream obj_stream = new MemoryStream();

            xmldoc.Save(obj_stream);

            return obj_stream.ToArray();

        }

    }
}
