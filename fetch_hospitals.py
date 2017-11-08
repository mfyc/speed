import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class fetch_hospitals(dml.Algorithm):
    contributor = 'mcaloonj'
    reads = []
    writes = ['mcaloonj.open_space']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mcaloonj', 'mcaloonj')

        url = "https://data.boston.gov/api/3/action/datastore_search?resource_id=6222085d-ee88-45c6-ae40-0c7464620d64"
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)

        print (r)



        repo.dropCollection("mcaloonj.hospitals")
        repo.createCollection("mcaloonj.hospitals")
        repo["mcaloonj.hospitals"].insert_many(r["hospitals"])
