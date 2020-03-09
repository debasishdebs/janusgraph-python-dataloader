from janusgraph_python.structure.JanusGraph import JanusGraph
from janusgraph_python.core.attribute.TextPredicate.Text import Text
from gremlin_python.process.graph_traversal import __
import datetime
from dateutil import parser


class VertexDAO(object):
    def __init__(self, schema, datamapper, print_per_record=500):
        self.schema = schema
        self.datamapper = datamapper
        self.nodes_in_records = []
        self.print_per_record = print_per_record
        pass

    def fit(self, nodes):
        """

        Args:
            nodes (list):

        Returns:

        """
        self.nodes_in_records = nodes
        return self

    def connect(self, url, port, bindings, graph):
        self.graph = JanusGraph().connect(url=url, port=port, bindings=bindings, graph=graph)
        self.traversal = self.graph.traversal()
        return self

    def push(self, deep=True):
        nodes_ref = []
        nodes_vMaps = []

        counter = 1
        record_num = 1
        start_time = datetime.datetime.now()

        for nodes in self.nodes_in_records:
            nodes_record = []
            nodes_vMaps_record = []

            for node in nodes:

                label = node["label"]
                updated_node = {k: v for k, v in node.items() if k != "label"}

                schema_for_label = [label_schema for label_schema in self.schema["nodes"] if label_schema["label"] == label][0]
                propertySchema = schema_for_label["propertyKeys"]

                constraints = self.get_constraints(label)

                if deep:
                    status, vMap = self.is_node_present(node, constraints, label, deep)
                else:
                    status = self.is_node_present(node, constraints, label, deep)

                if not status:
                    # When vertex is not present
                    v = self.traversal.addV(label)

                    for propName, propVal in updated_node.items():
                        data_type = propertySchema[propName]["dataType"]

                        # if isinstance(propVal, str) and "$" in propVal:
                        #     propVal = propVal.replace("$", "")

                        castedPropVal = self.map_value_to_data_type(data_type, propVal)
                        v = v.property(propName, castedPropVal)

                    try:
                        v = v.next()
                    except:
                        print("Got an error insert node # ", counter, v)
                        counter += 1
                        continue

                    if deep:
                        vMap = self.traversal.V(v.id).valueMap(True).next()

                else:
                    # Else, get the vertex
                    v = status

                if deep:
                    vMap = {str(k): v for k, v in vMap.items()}

                nodes_record.append(v)
                if deep:
                    nodes_vMaps_record.append(vMap)

                if counter % self.print_per_record == 0:
                    end_time = datetime.datetime.now()
                    print("Time taken to insert {} nodes till {} is {} ms".format(self.print_per_record, counter,
                                                                                  (end_time-start_time).total_seconds() * 1000))
                    start_time = datetime.datetime.now()

                counter += 1

            if record_num % self.print_per_record == 0:
                print("Finished inserting vertex record no ", record_num)

            record_num += 1

            nodes_ref.append(nodes_record)
            if deep:
                nodes_vMaps.append(nodes_vMaps_record)

        # self.graph.commit()
        # self.graph.close()
        if deep:
            return nodes_ref, nodes_vMaps
        else:
            return nodes_ref

    def map_value_to_data_type(self, data_type, value):
        if data_type == "Integer":
            try:
                v = int(value)
            except ValueError:
                v = 0
        elif data_type == "String":
            v = str(value)
        elif data_type == "Boolean":
            v = True if value.lower() == "true" else False
        elif data_type == "Date":
            try:
                v = parser.parse(value)
            except ValueError:
                v = datetime.datetime(0, 0, 0, 0, 0, 0)
                print("Couldn't parse: {} into DateTime. Defaulting to {}".format(value, v))
            except TypeError:
                if isinstance(value, datetime.datetime):
                    v = value
                else:
                    v = datetime.datetime(0, 0, 0, 0, 0, 0)

        else:
            print("Implemented only for Integer, String, Boolean, Date. Got {}. Passing default, String".format(data_type))
            v = str(value)
        return v

    def is_node_present(self, node, constraints, label, deep):

        subtraversal = []
        for constraintPropName in constraints:
            if constraintPropName in node and node[constraintPropName] != "":
                value = node[constraintPropName]
                # if isinstance(value, str) and "$" in value:
                #     value = str(value).replace("$", "")

                # if constraintPropName in ["userName"]:
                #     # To make it case insensitive when it comes to userName property while inserts
                #     subtraversal.append(__.has(constraintPropName, Text.textFuzzy(value)).has("node_label", label))
                # else:
                subtraversal.append(__.has(constraintPropName, value).has("node_label", label))

        v = self.traversal.V().or_(*subtraversal)

        try:
            v = v.next()
            if deep:
                vMap = self.traversal.V(v.id).valueMap(True).next()
                return v, vMap
            else:
                return v
        except:
            # print("Node not present:")
            if deep:
                return False, {}
            else:
                return False

    def get_constraints(self, label):
        constraints = self.datamapper["nodes"][label]["constraints"]
        uniqueness = constraints["unique"]

        constraintProperties = [x.strip() for x in uniqueness.split("or")]
        return constraintProperties
