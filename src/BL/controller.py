from typing import Any
import BL.model as md
from common import NeoDB

class actions:
    def fetch_agewise_stat(country:str,age_from:int,age_to:int)->list[md.AgeGroup_Stat]:
        try:
            neo = NeoDB()
            driver = neo.neoDriver()
            with driver:
                records,summary,keys = driver.execute_query(
                    f"""
                        MATCH(c:Country where c.name='{country}')
                        WITH c
                        MATCH (c)-[a:AGEWISE_TRAVELLED]->(ag:AgeRange 
                        Where ag.start >= {age_from} And ag.end <= {age_to})
                        RETURN c.name as Country,ag.name as Age_Range,a.val as Count
                        """,
                        database_= neo.DB_Name
                )
                result = [
                     md.AgeGroup_Stat(
                                    country = r['Country'],
                                    age_group = r['Age_Range'],
                                    val = r['Count']
                                 )
             for r in records]
                return result
             
        except ValueError as e:
            print(e)

    def fetch_all_Countries()->list[md.Country]:
        try:
            neo = NeoDB()
            driver = neo.neoDriver()
            with driver:
                records,summary,keys = driver.execute_query(
                    """
                        MATCH(c:Country) RETURN DISTINCT c.name as Countries
                        """,
                        database_= neo.DB_Name
                )
                result = [
                    md.Country(
                                name = r['Countries']
                                )
             for r in records]
                return result

        except ValueError as e:
            print(e)

    def fetch_airport_stat(countries:str,airports:str)->list[md.Airport_Stat]:
        try:
            neo = NeoDB()
            driver = neo.neoDriver()
            with driver:
                QRY=f"""
                        MATCH (c:Country where c.name = '{countries}')
                        WITH c
                        MATCH (c)-[v:VIA_AIRPORT]->(a:Airport where a.name = '{airports}')
                        RETURN c.name as Country,v.val as Count,a.name as Airport
                        """
                records,summary,keys = driver.execute_query(
                    QRY,
                        database_= neo.DB_Name,
                        countries = countries,airports=airports
                )
                result = [
                     md.Airport_Stat(
                                    country = r['Country'],
                                    airport_name = r['Airport'],
                                    val = r['Count']
                                 )
             for r in records]
                return result
             
        except ValueError as e:
            print(e)

if __name__ == "__main__":
    data = actions.fetch_airport_stat('Canada','Delhi')
    print(data)
    #pass