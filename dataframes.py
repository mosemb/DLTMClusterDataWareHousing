from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("DatawarehouseSpark Application") \
    .getOrCreate()

#Input the data
inputFileHire ='/user/user17/mosedata_proj/input/client_hiring_dt.csv'
inputFileBio ='/user/user17/mosedata_proj/input/client_bio_dt.csv'
inputFileCom = '/user/user17/mosedata_proj/input/client_communication_dt.csv'
inputFileAct = '/user/user17/mosedata_proj/input/client_activities_dt.csv'
inputFileFact = '/user/user17/mosedata_proj/input/client_fact_ft.csv'

#Create data frames from sources. Tables were converted to csvs now being converted to dataframes
dfHire =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileHire)
dfBio =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileBio)
dfCom =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileCom)
dfAct =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileAct)
dfFact =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileFact)

# Print the schema of the DataFrames
dfHire.printSchema() 
dfBio.printSchema()
dfCom.printSchema()
dfAct.printSchema()
dfFact.printSchema()

#Clients from different service branches
dfBio.groupBy('service_branch__c').count().show()

#Types of jobs the guys got hired in
dfHire.groupBy('job_function_hired_in__c').count().show()

#Temporary objects from which sql statements are run for each dataframe
dfHire.createOrReplaceTempView("dfHire_sql")
dfBio.createOrReplaceTempView("dfBio_sql")
dfCom.createOrReplaceTempView("dfCom_sql")
dfAct.createOrReplaceTempView("dfAct_sql")
dfFact.createOrReplaceTempView("dfFAct_sql")

#Mixed information from all 5 tables of the database
spark.sql(''' Select hires.hired, bio.service_rank__c , 
              bio.service_branch__c , fact.yearsinservice, fact.reg_afterservice_years,
              com.responsive__c, act.finalized_hhusa_revised_resume_on_file__c as resume_done
              from dfHire_sql hires
              inner join dfBio_sql bio on bio.id = hires.id 
              inner join dfFAct_sql fact on fact.id = hires.id
              inner join dfCom_sql com on com.id = hires.id
              inner join dfAct_sql act on act.id = hires.id
              where hires.hired = "1"
              
              ''').show()

#Account create date by month of the year
spark.sql( ''' SELECT count(id) , 
               Extract(Month From create_ddate) as month 
               From  dfAct_sql  
               group by month 
               ORDER BY month asc ''' 
           ).show()


#Registration for services by year
#spark.sql( """SELECT count(id) , 
#Extract(Year From create_ddate) as year_registered 
#              From  dfAct_sql  
#              group by year_registered
#              ORDER BY year_registered  """).show()

#Hired by Service branch
spark.sql(""" Select count(hires.id) as hired,bio.service_branch__c 
              From dfHire_sql hires 
              inner join dfBio_sql bio on bio.id = hires.id
              Where hires.hired = 1
              group by bio.service_branch__c
              order by hired asc
              """).show()

#Not hired by Service Branch
spark.sql(""" Select count(hires.id) as Not_hired,bio.service_branch__c 
              From dfHire_sql hires 
              inner join dfBio_sql bio on bio.id = hires.id
              Where hires.hired = 0
              group by bio.service_branch__c
              order by Not_hired asc
              """).show()

#Hired by rank
spark.sql(""" Select count(hires.id) as hired,bio.service_rank__c
              From dfHire_sql hires 
              inner join dfBio_sql bio on bio.id = hires.id
              Where hires.hired = 0
              group by bio.service_rank__c
              order by hired desc
              """).show()

#Prefered method of contact for the hired
spark.sql(""" select count(com.id) as count, com.preferred_method_of_contact__c 
              from dfCom_sql com 
              inner join dfHire_sql hires on com.id = hires.id
              where hires.hired = 1
              group by com.preferred_method_of_contact__c 
              order by count
          """).show()

#Prefered method of contact for the Unhired
spark.sql(""" select count(com.id) as count, com.preferred_method_of_contact__c 
              from dfCom_sql com 
              inner join dfHire_sql hires on com.id = hires.id
              where hires.hired = 0
              group by com.preferred_method_of_contact__c 
              order by count
          """).show()

# Stop session.
spark.stop()