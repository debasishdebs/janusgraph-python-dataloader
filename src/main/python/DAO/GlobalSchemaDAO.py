from .SchamaDAO import SchemaDAO
from janusgraph_python.structure.JanusGraph import JanusGraph


class GlobalSchemaDAO(object):
    def __init__(self):
        self.schemas = []
        self.push = False
        self.print = not self.push
        pass

    def fit(self, schemas):
        """

        Args:
            schemas (list):

        Returns:

        """
        self.schemas = schemas
        return self

    def connect(self, url, port, graph):
        self.print = False
        graph = JanusGraph().connect(url=url, port=port, graph=graph)
        self.mgmt = graph.openManagement()
        print("Connection established")
        return self

    def create(self):

        return
