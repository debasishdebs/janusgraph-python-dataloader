from janusgraph_python.structure.JanusGraph import JanusGraph


class SchemaDAO(object):
    def __init__(self, schema):
        self.schema = schema
        self.print = True
        pass

    def connect(self, url, port, graph):
        self.print = False
        graph = JanusGraph().connect(url=url, port=port, graph=graph)
        self.mgmt = graph.openManagement(True)
        return self

    def create(self):

        properties = self.retrieve_property_keys()
        self.make_property_keys(properties)
        print("Added all property keys")

        edge_labels = list(set([x["label"] for x in self.schema["edges"]]))
        self.make_edge_labels(edge_labels)
        print("Added all edge labels")

        vertex_labels = list(set([x["label"] for x in self.schema["nodes"]]))
        self.make_vertex_labels(vertex_labels)
        print("Added all vertex labels")

        self.make_composite_indexes()
        print("Added all composite indexes")

        self.make_mixed_indexes()
        print("Added all mixed indexes")

        self.make_vertex_centric_index()
        print("Added all vertex centric indexes")
        return self

    def make_property_keys(self, properties):
        for property_key, property_key_info in properties.items():
            cardinality = property_key_info["cardinality"]
            data_type = property_key_info["dataType"]

            self.mgmt.propertyKeyBuilder().makePropertyKey(property_key).dataType(data_type).cardinality(cardinality).make()
            print("Build {} property key".format(property_key))
        return self

    def make_edge_labels(self, edge_labels):
        for edge_label in edge_labels:
            schema_for_edge = [x for x in self.schema["edges"] if x["label"] == edge_label][0]
            multiplicity = schema_for_edge["multiplicity"] if "multiplicity" in schema_for_edge else "MULTI"
            self.mgmt.edgeLabelBuilder().makeEdgeLabel(edge_label).multiplicity(multiplicity).make()
            print("Build {} edge label".format(edge_label))
        return self

    def make_vertex_labels(self, vertex_labels):
        for vertex_label in vertex_labels:
            self.mgmt.vertexLabelBuilder().makeVertexLabel(vertex_label).make()
            print("Build {} vertex label".format(vertex_label))
        return self

    def make_vertex_centric_index(self):
        edge_indexes = self.schema["index"]["vertexCentricIndex"]

        for index_info in edge_indexes:
            index_name = index_info["name"]
            edge = index_info["edge"]
            direction = index_info["direction"] if "direction" in index_info else False
            order = index_info["order"] if "order" in index_info else False
            properties = index_info["properties"]

            index = self.mgmt.buildVertexCentricIndex(index_name).addEdge(edge).on(properties)

            if direction:
                index = index.direction(direction)
            if order:
                index = index.order(order)

            idx_name = index.make()

            self.mgmt.awaitGraphIndexStatus(idx_name).call()
            self.mgmt.commit()

            print("Build {} Vertex Centric index".format(index_info))

        return self

    def make_mixed_indexes(self):
        mixed_indexes = self.schema["index"]["mixedIndex"]

        for index_info in mixed_indexes:
            index_name = index_info["name"]
            index_only = index_info["indexOnly"] if "indexOnly" in index_info else False
            properties = index_info["properties"]
            elements = index_info["element"]

            if len(elements) == 1:
                index = self.mgmt.buildMixedIndex(index_name, elements[0])
                for key in properties:
                    index = index.addKey(key, "TEXT")

                if index_only:
                    index = index.indexOnly(index_only)

                idx_name = index.make()

                self.mgmt.awaitGraphIndexStatus(idx_name).call()
                self.mgmt.commit()
            else:
                for element in elements:
                    index = self.mgmt.buildMixedIndex(index_name + "_{}".format(element[0]), element)

                    for key in properties:
                        index = index.addKey(key, "TEXT")

                    if index_only:
                        index = index.indexOnly(index_only)

                    idx_name = index.make()

                    self.mgmt.awaitGraphIndexStatus(idx_name).call()
                    self.mgmt.commit()

            print("Build {} mixed index".format(index_name))

        return self

    def make_composite_indexes(self):
        composite_indexes = self.schema["index"]["compositeIndex"]

        for index_info in composite_indexes:
            index_name = index_info["name"]
            index_only = index_info["indexOnly"] if "indexOnly" in index_info else False
            unique = True if index_info["unique"] == "true" else False
            properties = index_info["properties"]
            elements = index_info["element"]

            if len(elements) == 1:
                index = self.mgmt.buildCompositeIndex(index_name, elements[0])
                for key in properties:
                    index = index.addKey(key)
                if unique:
                    index = index.unique()
                if index_only:
                    index = index.indexOnly(index_only)
                idx_name = index.make()

                self.mgmt.awaitGraphIndexStatus(idx_name).call()
                self.mgmt.commit()

                print("Build {} composite index".format(idx_name))
            else:
                for element in elements:
                    index = self.mgmt.buildCompositeIndex(index_name + "_{}".format(element[0]), element)
                    for key in properties:
                        index = index.addKey(key)
                    if unique:
                        index = index.unique()
                    if index_only:
                        index = index.indexOnly(index_only)
                    idx_name = index.make()

                    self.mgmt.awaitGraphIndexStatus(idx_name).call()
                    self.mgmt.commit()

                    print("Build {} composite index".format(idx_name))

        return self

    def retrieve_property_keys(self):
        property_keys = {}

        # First we fetch properties for nodes
        for node_schema in self.schema["nodes"]:
            properties = node_schema["propertyKeys"]

            for property_key, property_key_info in properties.items():
                if property_key not in property_keys:
                    property_keys[property_key] = property_key_info
                else:
                    if property_key_info == property_keys[property_key]:
                        print("Same attributes for property key ", property_key)
                    else:
                        print("AttributeError(\"Duplicate found for \", property_key)")

        # Second we fetch properties for nodes
        for edge_schema in self.schema["edges"]:
            properties = edge_schema["propertyKeys"]

            for property_key, property_key_info in properties.items():
                if property_key not in property_keys:
                    property_keys[property_key] = property_key_info
                else:
                    if property_key_info == property_keys[property_key]:
                        print("Same attributes for property key ", property_key)
                    else:
                        print("AttributeError(\"Duplicate found for \", property_key)")

        return property_keys


if __name__ == '__main__':
    import json
    schema_files = [
        "../../resources/python-schema/schema_guardduty_traffic.json",
        "../../resources/python-schema/schema_guardduty_events.json",
        "../../resources/python-schema/schema_windows_v2.json"
    ]

    for schema_file in schema_files:
        # schema_file = "../../resources/python-schema/schema_guardduty_traffic.json"
        print(100*"*")
        print(30*" " + "Schema for " + schema_file)
        schema = json.load(open(schema_file))
        # print("Schema loaded")
        obj = SchemaDAO(schema)
        obj.connect(url="10.10.110.128", port="8182", graph="graph")
        obj.create()
        print(30*" " + "Schema finished for " + schema_file)
        print(100*"*")
