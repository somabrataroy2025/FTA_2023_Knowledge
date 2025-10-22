import BL.model as md
from common import NeoDB

class actions:
    def fetch_agewise_stat()->list[md.AgeGroup_Stat]:
        try:
            neo = NeoDB()
            driver = neo.neoDriver()
            with driver:
                records,summary,keys = driver.execute_query(
                    """
                        MATCH p=(e1)-[r:AGEWISE_TRAVELLED]->(e2) 
                        RETURN e1.name as Country,e2.name AS Age_Group,r.val as Count
                        """,
                        database_= neo.DB_Name
                )
                result = [
                     md.AgeGroup_Stat(
                                    country = r['Country'],
                                    age_group = r['Age_Group'],
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


if __name__ == "__main__":
    #data = BL.fetch_agewise_stat()
    #print(data)
    pass