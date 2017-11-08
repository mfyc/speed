import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class fetch_accidents(dml.Algorithm):
        contributor = 'mcaloonj'
        reads = []
        writes = ['mcaloonj.accidents']

        @staticmethod
        def execute(trial=False):
            startTime = datetime.datetime.now()
            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('mcaloonj', 'mcaloonj')

            url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000'

            response = requests.get(url)
            r = response.json()
            s = json.dumps(r, sort_keys=True, indent=2)
            #print (s)
            #print ()

            repo.dropCollection("accidents")
            repo.createCollection("accidents")

            repo['mcaloonj.accidents'].insert_many(r["result"]["records"])
            repo['mcaloonj.accidents'].metadata({'complete':True})
            #print(repo['mcaloonj.accidents'].metadata())

            repo.logout()

            endTime = datetime.datetime.now()

            return {"start":startTime, "end":endTime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('mcaloonj','mcaloonj')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
            doc.add_namespace('dbg','https://data.boston.gov/api/3/action/datastore_search')

            this_script = doc.agent('alg:mcaloonj#fetch_accidents', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extenstion':'py'})
            resource = doc.entity('dbg:'+str(uuid.uuid4()), {'prov:label': 'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extenstion':'json'})

            get_accidents = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_accidents, this_script)

            doc.usage(get_accidents, resource, startTime, None,
                      {prov.model.PROV_TYPE:'ont:Retrieval',
                      'ont:Query':'?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&q=Motor%20Vehicle%20Accident%20Response&limit=50000'
                      }
                      )

            accidents = doc.entity('dat:mcaloonj#accidents', {prov.model.PROV_LABEL:'Accidents',prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(accidents, this_script)
            doc.wasGeneratedBy(accidents, get_accidents, endTime)
            doc.wasDerivedFrom(accidents, resource, get_accidents, get_accidents, get_accidents)

            repo.logout()
            return doc
'''
fetch_accidents.execute()
doc = fetch_accidents.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
##eof
