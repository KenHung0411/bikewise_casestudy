# I will present my casestudy through a demo, 
#### about the architect please refer to the png file bikewise_case_study.png

The architect here is to specifically sloved these problems in the case study here I make a checklist to review the tasks:
  1. new data has to ingested on a daily basis -> **(checked)**
      - By applying airflow I can run whenever I want easily
  2. output must be big data friendly. What do we mean by big data friendly? -> **(checked)**
      - There are two phases that data can be touched for users as a "row" 
          1. Through datalake (s3 + athena) of raw data (less formated)
          2. Through pre-process(aggregate) data sit in data warehouse (Usually this could be Redshift or RDS)
          3. Through pre-process(EMR or Glue) data write it back to datalake(S3) waiting for the further uses.
  3. Suppose that another team wants to use this data to do some text analyses ondata. 
     How would you set up an architecture so that other teams do not need to worry about the data ingestion? - **(checked)**
     - Everything are handled(Airflow) users only need to have the knowlege of SQL and either do it on Athena or Redshift(or RDS)
  4. The data that we are reading is updated from time to time. 
     Write a code or write the design (architecture) how to build a history of the incidents. 
     In other words, how do we keep all versions of the incidents in our data storage? 
     Such as any field of the incident changed and we need to keep track of this incident again. -  **(checked)**
     - With my designed I kept all the data on S3 with timestamp partition hence with where clause I'm able to trace any history data. 
  
