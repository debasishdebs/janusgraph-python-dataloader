import sys, os

sys.path.append(os.path.abspath(os.getcwd() + "\\..\\"))

from DAO.VertexDAO import VertexDAO
from DAO.EdgeDAO import EdgeDAO
from DAO.SchamaDAO import SchemaDAO
from Extractor.DataExtractor import DataExtractor
from utils.RemoveDuplicates import RemoveDuplicates
import time
import json
import datetime
from utils.datetime_handler import datetime_handler


class DataLoader(object):
    def __init__(self, extractor):
        self.extractor = extractor

        self.schema = dict()
        self.datamapper = dict()
        self.records_file = []
        self.records = dict()
        self.filtered_records = list()
        self.nodes = []
        self.edges = []
        pass

    def fit(self, schema, datamapper):
        self.schema = schema
        self.datamapper = datamapper
        return self

    def add_data(self, records_file):
        self.records_file = records_file
        self.filtered_records = json.load(open(records_file))
        pass

    def connect(self, url, port, graph, bindings):
        self.url = url
        self.port = port
        self.bindings = bindings
        self.graph = graph

        print("Schema Groovy")
        # self.create_schema()
        return self

    def extract(self):
        extractor_obj = self.extractor(self.schema)
        extractor_obj.fit(self.datamapper, self.filtered_records)
        self.nodes, self.edges = extractor_obj.get_nodes_and_edges()

        print(20*"=")
        print("Extraction complete")
        length = sum([len(x) for x in self.nodes])
        print("Nodes: Records: {}. Nodes (Inc duplicates): {}".format(len(self.nodes), length))
        length = sum([len(x) for x in self.edges])
        print("Edges: Records: {}. Edges (Inc duplicates): {}".format(len(self.edges), length))
        print(20*"=")
        return self

    def deduplicate(self):
        print("Removing duplicates now")
        obj = RemoveDuplicates(self.datamapper)
        obj.fit(self.nodes, self.edges)

        self.nodes, self.edges = obj.deduplicate_nodes_and_edges()

        print(20*"=")
        print("Deduplication complete")
        length = sum([len(x) for x in self.nodes])
        print("Nodes: Records: {}. Nodes (W/O duplicates): {}".format(len(self.nodes), length))
        length = sum([len(x) for x in self.edges])
        print("Edges: Records: {}. Edges (W/O duplicates and aggregated): {}".format(len(self.edges), length))
        print(20*"=")

        return self

    def create_schema(self):
        schema_dao = SchemaDAO(self.schema)
        schema_dao.connect(url=self.url, port=self.port, bindings=self.bindings)
        schema_dao.create()
        return self

    def push(self):
        print("Inserting nodes first")
        st = datetime.datetime.now()
        node_dao = VertexDAO(self.schema, self.datamapper, 1000)
        node_dao.fit(self.nodes)
        node_dao.connect(url=self.url, port=self.port, bindings=self.bindings, graph=self.graph)
        nodes_ref, node_maps = node_dao.push()
        e = datetime.datetime.now()
        print("Finished inserting nodes in {} seconds. Sleeping 5 sec now".format((e-st).total_seconds()))

        time.sleep(5)

        print("Inserting edges now")
        st = datetime.datetime.now()
        edge_dao = EdgeDAO(self.schema, self.datamapper, 1000)
        edge_dao.fit(self.edges)
        edge_dao.connect(url=self.url, port=self.port, graph=self.graph, bindings=self.bindings)
        edge_dao.push(node_maps)
        e = datetime.datetime.now()
        print("Finished inserting edges in {} seconds".format((e-st).total_seconds()))

    def save(self, fnames):
        """

        Args:
            fnames (dict):

        Returns:

        """

        node_fname = fnames["node"]
        edge_fname = fnames["edge"]

        json.dump(self.nodes, open(node_fname, "w+"), indent=2, default=datetime_handler)
        json.dump(self.edges, open(edge_fname, "w+"), indent=2, default=datetime_handler)

        return self


if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_mapper_file')
    parser.add_argument('--schema_file')
    parser.add_argument('--data_file')
    parser.add_argument('--config_file')
    args = parser.parse_args()

    data_mapper_file = args.data_mapper_file
    data_file = args.data_file
    config_file = args.config_file
    schema_file = args.schema_file

    start = datetime.datetime.now()

    config = json.load(open(config_file))

    url = config["JG_URL"]
    port = config["JG_PORT"]
    graph = config["JG_GRAPH"]
    bindings = config["JG_TRAVERSAL"]

    #data_file = os.path.abspath(data_file)
    #schema_file = os.path.abspath(schema_file)
    #datamapper_file = os.path.abspath(data_mapper_file)

    records_files = [data_file]

    schema = json.load(open(schema_file))
    datamapper = json.load(open(data_mapper_file))

    print("Loading Sample Data\n")

    extractor = DataExtractor

    for file in records_files:
        print("Starting to push records from ", file)
        start = datetime.datetime.now()

        obj = DataLoader(extractor)
        obj.fit(schema, datamapper)
        obj.add_data(file)
        obj.connect(url, port, graph, bindings)
        obj.extract()
        obj.deduplicate()
        obj.push()

        end_time = datetime.datetime.now()
        print("Successfully pushed all data from {} in {} seconds".format(file, (end_time-start).total_seconds()))
