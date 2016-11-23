# Query AWS Redshift from SAP Netweaver
[AWS Redshift] (https://aws.amazon.com/redshift/?sc_channel=PS&sc_campaign=acquisition_US&sc_publisher=google&sc_medium=redshift_b&sc_content=redshift_bmm&sc_detail=%2Baws%20%2Bredshift&sc_category=redshift&sc_segment=108322330362&sc_matchtype=b&sc_country=US&s_kwcid=AL!4422!3!108322330362!b!!g!!%2Baws%20%2Bredshift&ef_id=WDYOcwAABDktuUPd:20161123214731:s) is a petabyte-scale data warehouse that's a fully managed and optimized for large-scale datasets.  Deep down, its built upon massive parallel processing (MPP) technology from ParAccel with a PostgreSQL flavor.  Amazon provides native ODBC and JDBC drivers so you can connect just about any business intellegence or ETL tools up to RedShift.
Like most things on AWS, you decide how much horsepower and storage you want to pay.  For about $1000 per terabyte per year, you can provide a fault-tollerant, encrypted data warehouse to unlimited users.  If you've outgrown your existing operational data stores and are considering the creation of an Enterprise Data Warehouse, be sure to give Redshift a look along with the usual suspects like Teradata, Netizza, Exadata, and the like.
In conversations with other SAP customers, they do seem to be interested in Redshift, but not exactly sure how to integrate it.  We decided to build a demonstration of one way to integrate SAP Netweaver with Redshift--specifically to allow ABAP code to execute SQL against AWS Redshift.
There are all sorts of methods one could use to integrate the two platforms--creating an ODATA or REST layer for example--but we wanted to try a method that was more "SAP-native".

## Objectives
- Allow ABAP code to submit queries--really any SQL statements--to Redshift and receive the recordset back.
- Does not require customization or non-standard access to Redshift; preserve the Redshift security, logging and monitoring models 
- Does not require customization to SAP Netweaver (other than the ABAP z-code we'll write)

## The Design
For this scenario, we are going to image that our company has recently aquired another company along with about 10 years of detailed customer interactions.  We could try to load this data into our existing SAP ECC system, but the space requirements and performance impacts would be immense.  We need a way to allow the sales people to look up customer history from within the existing tools they use for customer order management--SAP ECC screens with Personas frontends.
Since we're using Personas, we have a good degree of freedom in how we present the data to the sales people, but we still have to do the heavy lifting in ABAP.  *Oh, and did I mention that we have only 2 weeks until the Board of Directors wants to announce the aquisition to the press?*  No sweat.
 

we are going to imagine our company runs our entire Customer Activity Repository on Redshift.  It includes every interaction, every product ever bought, every inbound or outbound phone call with our customers.  

## The Steps
1.
2.
3.
4.





