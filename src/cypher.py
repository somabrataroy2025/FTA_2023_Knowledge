from common import NeoDB
from pandas import DataFrame
from neo4j import RoutingControl

class Cypher():
    QRY_DB_Create = """
                :use system;
                CREATE DATABASE fta2023 IF NOT EXISTS
    """
    def mergeRegion(self,df: DataFrame):
        try:
            neo = NeoDB()
            driver = neo.neoDriver()
            with driver:
                for index,row in df.iterrows():
                    records,summary,keys = driver.execute_query(
                        """
                            MERGE(r:Region {name : $row})
                            """,
                            database_= neo.DB_Name,
                            routing_= RoutingControl.WRITE,
                            row = row['Region']
                        )
        except ValueError as e:
            print(e)

    def mergeRegionCountry(self,df: DataFrame):
        try:
            neo = NeoDB()
            driver = neo.neoDriver()
            with driver:
                for index,row in df.iterrows():
                    records,summary,keys = driver.execute_query(
                        """
                            MERGE (c:Country {name: $row.country})
                            WITH c
                            MATCH (r:Region {name : $row.region})
                            MERGE (c)-[:ORIGINATES]->(r)
                            """,
                            database_= neo.DB_Name,
                            row = {'region':row['Region'],'country':row['Country']}
                    )
        except ValueError as e:
            print(e)
        
    def createStaticDataSet(self):
        QRY_Quarter = """
            MERGE(q1:Quarter {name:'Q1'}) set q1.start = date('2022-01-01') set q1.end = date('2022-03-31')
            MERGE(q2:Quarter {name:'Q2'}) set q2.start = date('2022-04-01') set q2.end = date('2022-06-30')
            MERGE(q3:Quarter {name:'Q3'}) set q3.start = date('2022-07-01') set q3.end = date('2022-09-30')
            MERGE(q4:Quarter {name:'Q4'}) set q4.start = date('2022-10-01') set q4.end = date('2022-12-31')
            MERGE(:Airport {name : 'Bangalore'})
            MERGE(:Airport {name : 'Chennai'})
            MERGE(:Airport {name : 'Cochin'})
            MERGE(:Airport {name : 'Delhi'})
            MERGE(:Airport {name : 'Haridaspur'})
            MERGE(:Airport {name : 'Hyderabad'})
            MERGE(:Airport {name : 'Kolkata'})
            MERGE(:Airport {name : 'Mumbai'})
            MERGE(:Airport {name : 'Other'})
            MERGE(:Gender {name:"Male"})
            MERGE(:Gender {name:"Female"})
            MERGE(a:AgeRange {name : '0-14'}) set a.start = 0 set a.end = 14
            MERGE(b:AgeRange {name : '15-24'})  set b.start = 15 set b.end = 24
            MERGE(c:AgeRange {name : '25-34'})  set c.start = 25 set c.end = 34
            MERGE(d:AgeRange {name : '35-44'})  set d.start = 35 set d.end = 44
            MERGE(e:AgeRange {name : '45-54'})  set e.start = 45 set e.end = 54
            MERGE(f:AgeRange {name : '55-64'})  set f.start = 55 set a.end = 64
            MERGE(g:AgeRange {name : '65 and Above'})  set g.start = 65 set g.end = 0
            """
        neo = NeoDB()
        driver = neo.neoDriver()
        with driver:
            record,summary,keys = driver.execute_query(
                QRY_Quarter, 
                database_= neo.DB_Name,
                routing_= RoutingControl.WRITE
            )
            print(record)
            print(summary)
            print(keys)

    def mergeFTACount(self,df:DataFrame):
        neo = NeoDB()
        driver = neo.neoDriver()
        with driver:
            for index,row in df.iterrows():
                record,summary,keys = driver.execute_query(
                """
                    MATCH(c:Country {name : $row.country})
                    MATCH(q:Quarter {name : $row.key})
                    MERGE (c)-[r:FOOT_FALL]->(q)
                        set r.val = $row.val

                """, 
                database_= neo.DB_Name,
                row = {
                       'country':row['Country'],
                       'key' : row['Key'],
                       'val' : row['Val']}
            )
    def mergeFTAAirport(self,df:DataFrame):
        neo = NeoDB()
        driver = neo.neoDriver()
        with driver:
            for index,row in df.iterrows():
                record,summary,keys = driver.execute_query(
                """
                    match (c:Country {name:$row.country})
                    match (a:Airport {name:$row.key})
                    with c,a
                    call apoc.do.when($row.val > 0,'merge (c)-[r:VIA_AIRPORT]->(a)  
                    ON CREATE SET r.createDate = datetime()
                    ON MATCH SET r.updateDate = datetime(),r.val = val 
                    ','',{c:c, a:a, val:$row.val})
                    YIELD value
                    RETURN value
                """, 
                database_= neo.DB_Name,
                row = {
                       'country':row['Country'],
                       'key' : row['Key'],
                       'val' : row['Val']}
            )
                
    def mergeFTAGender(self,df:DataFrame):
        neo = NeoDB()
        driver = neo.neoDriver()
        with driver:
            for index,row in df.iterrows():
                record,summary,keys = driver.execute_query(
                """
                    MATCH(c:Country {name : $row.country})
                    MATCH(a:Gender {name : $row.key})
                    MERGE (c)-[r:GENDER_TRAVELLED]->(a)
                        set r.val = $row.val

                """, 
                database_= neo.DB_Name,
                row = {
                       'country':row['Country'],
                       'key' : row['Key'],
                       'val' : row['Val']}
            )

    def mergeFTAAgeWise(self,df:DataFrame):
        neo = NeoDB()
        driver = neo.neoDriver()
        with driver:
            for index,row in df.iterrows():
                record,summary,keys = driver.execute_query(
                """
                    MATCH(c:Country {name : $row.country})
                    MATCH(a:AgeRange {name : $row.key})
                    MERGE (c)-[r:AGEWISE_TRAVELLED]->(a)
                        set r.val = $row.val

                """, 
                database_= neo.DB_Name,
                row = {
                       'country':row['Country'],
                       'key' : row['Key'],
                       'val' : row['Val']}
            )

    
if __name__=="__main__":
    Cypher().createStaticDataSet()