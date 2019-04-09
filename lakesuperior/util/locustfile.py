import random

from os import environ
from uuid import uuid4

import requests

from locust import HttpLocust, TaskSet, task
from rdflib import Graph, URIRef

from lakesuperior.util.generators import random_graph, random_image

ldp_root = environ.get(
    'FCREPO_BENCHMARK_ROOT', 'http://localhost:8000/ldp/pomegranate'
)
print('Retrieving LDP graphs. Be patient, this may take a while...')
rsp = requests.request('GET', ldp_root)
root_gr = Graph().parse(data=rsp.text, format='ttl')
subjects = {*root_gr.objects(
    None, URIRef('http://www.w3.org/ns/ldp#contains')
)}

class Graph(TaskSet):

    @task(1)
    def ingest_graph(self):
        uri = f'{ldp_root}/{uuid4()}'
        data = random_graph(200, ldp_root).serialize(format='ttl')
        headers = {'content-type': 'text/turtle'}
        rsp = self.client.request('PUT', uri, data=data, name='random_ingest', headers=headers)


    @task(50)
    def request_graph(self):
        uri = str(random.sample(subjects, 1)[0])
        self.client.request('get', uri, name='random_get')


class LsupSwarmer(HttpLocust):
    task_set = Graph
    min_wait = 50
    max_wait = 500

