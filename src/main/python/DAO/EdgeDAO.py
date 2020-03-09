from janusgraph_python.structure.JanusGraph import JanusGraph
from gremlin_python.process.graph_traversal import __
from dateutil import parser
import datetime
from utils.datetime_handler import datetime_handler
from gremlin_python.driver.protocol import GremlinServerError
from janusgraph_python.core.attribute.TextPredicate.Text import Text


class EdgeDAO(object):
    def __init__(self, schema, datamapper, print_per_record=500):
        self.schema = schema
        self.datamapper = datamapper
        self.edges_in_records = []
        self.print_per_record = print_per_record

    def fit(self, edges):
        """

        Args:
            edges (list):

        Returns:

        """
        self.edges_in_records = edges
        return self

    def connect(self, url, port, bindings, graph):
        self.graph = JanusGraph().connect(url=url, port=port, bindings=bindings, graph=graph)
        self.traversal = self.graph.traversal()
        return self

    def push_deep_slow_without_node_ref(self):
        edges_ref = []
        counter = 1

        success_cnt = 0
        failed_cnt = 0

        start_time = datetime.datetime.now()

        for edges in self.edges_in_records:
            edges_record = []

            for edge in edges:

                edge_label = edge["label"]
                left = edge["left"]
                right = edge["right"]

                leftPropName = left["propName"].split(".")[1]
                leftLabelName = left["propName"].split(".")[0]
                leftVal = left["propVal"]

                rightPropName = right["propName"].split(".")[1]
                rightLabelName = right["propName"].split(".")[0]
                rightVal = right["propVal"]

                updated_edge = {k: v for k, v in edge.items() if k not in ["label", "left", "right"]}

                schema_for_label = [label_schema for label_schema in self.schema["edges"] if label_schema["label"] == edge_label][0]
                propertySchema = schema_for_label["propertyKeys"]
                # print(self.traversal.V().has(leftPropName, leftVal).hasLabel(leftLabelName))
                # print(self.traversal.V().has(leftPropName, leftVal).hasLabel(leftLabelName).next())
                # try:
                #     leftNode = self.traversal.V().has(leftPropName, leftVal).has("node_label", leftLabelName).next()
                #     rightNode = self.traversal.V().has(rightPropName, rightVal).has("node_label", rightLabelName).next()
                # except StopIteration:
                #     print("Vertex 1 or Vertex 2 not found. Edge label: ", edge_label)
                #     print("Left: {}.{}={}".format(leftLabelName, leftPropName, leftVal),
                #           " Right: {}.{}={}".format(rightLabelName, rightPropName, rightVal), " Label: ", edge_label)
                #     continue

                if leftPropName in ["hostname", "userName"] and rightPropName in ["hostname", "userName"]:

                    e = self.traversal.V().has(leftPropName, Text.textFuzzy(leftVal)).has("node_label",
                                      leftLabelName).as_("from").V().has(rightPropName, Text.textFuzzy(rightVal)).\
                                        has("node_label", rightLabelName).addE(edge_label).from_("from")

                elif leftPropName in ["hostname", "userName"] and rightPropName not in ["hostname", "userName"]:
                    e = self.traversal.V().has(leftPropName, Text.textFuzzy(leftVal)).has("node_label",
                                leftLabelName).as_("from").V().has(rightPropName, rightVal). \
                                has("node_label", rightLabelName).addE(edge_label).from_("from")

                elif leftPropName not in ["hostname", "userName"] and rightPropName in ["hostname", "userName"]:
                    e = self.traversal.V().has(leftPropName, leftVal).has("node_label",
                                    leftLabelName).as_("from").V().has(rightPropName, Text.textFuzzy(rightVal)). \
                                    has("node_label", rightLabelName).addE(edge_label).from_("from")

                else:
                    e = self.traversal.V().has(leftPropName, leftVal).has("node_label",
                                    leftLabelName).as_("from").V().has(rightPropName, rightVal). \
                                    has("node_label", rightLabelName).addE(edge_label).from_("from")

                for propName, propVal in updated_edge.items():
                    data_type = propertySchema[propName]["dataType"]

                    castedPropVal = self.map_value_to_data_type(data_type, propVal)
                    e = e.property(propName, castedPropVal)

                try:
                    e = e.next()
                    edges_record.append(e)
                    success_cnt += 1
                except GremlinServerError:
                    failed_cnt += 1
                    continue

                if counter % self.print_per_record == 0:
                    end_time = datetime.datetime.now()
                    print("Time taken to insert {} edges till {} is {} ms. Success count {} Fail count {}".format(self.print_per_record, counter,
                                                                                  (end_time-start_time).total_seconds() * 1000, success_cnt, failed_cnt))

                    start_time = datetime.datetime.now()

                counter += 1

            edges_ref.append(edges_record)

        return edges_ref

    def push_shallow_fast_with_ref(self, node_maps):
        edge_ids = []
        counter = 1
        record_num = 1
        # idx = 0
        print("Total records: ", len(self.edges_in_records))

        label_key = "T.label"
        id_key = "T.id"
        node_map_dict = dict()
        start_time = datetime.datetime.now()

        sucess_cnt = 0
        fail_cnt = 0

        idx = 1

        for node_map in node_maps:
            for node in node_map:
                label = node[label_key]

                if label == "user":
                    propName = "userName"
                    propVal = node[propName][0]
                elif label == "IP":
                    propName = "ip" if "ip" in node and node["ip"] != "" else "hostname" if "hostname" in node and node["hostname"] != "" else "domainControllerHostname"
                    propVal = node[propName][0]
                elif label == "process":
                    propName = "fullFileName"
                    propVal = node[propName][0]
                elif label == "email":
                    propName = "messageID"
                    propVal = node[propName][0]
                else:
                    raise NotImplementedError("Currently implemented only for label user, email, process and IP")

                key = "label:{}_and_{}:{}".format(label, propName, propVal)
                key = key.lower()

                idx += 1

                node_map_dict[key] = node

        print("Re data structured node vmaps. No elements before: {} and now: {}".format(sum([len(x) for x in node_maps]), len(node_map_dict.keys())))

        for edges in self.edges_in_records:
            edges_record = []

            for edge in edges:

                label = edge["label"]
                left = edge["left"]
                right = edge["right"]

                # This is list of all nodes added/retrieved in that record

                updated_edge = {k: v for k, v in edge.items() if k not in ["label", "left", "right"]}
                schema_for_label = [label_schema for label_schema in self.schema["edges"]
                                    if label_schema["label"] == label][0]
                propertySchema = schema_for_label["propertyKeys"]

                left_label = left["propName"].split(".")[0]
                left_prop = left["propName"].split(".")[1]
                left_val = left["propVal"]
                right_label = right["propName"].split(".")[0]
                right_prop = right["propName"].split(".")[1]
                right_val = right["propVal"]

                left_key = "label:{}_and_{}:{}".format(left_label, left_prop, left_val)
                right_key = "label:{}_and_{}:{}".format(right_label, right_prop, right_val)

                left_key = left_key.lower()
                right_key = right_key.lower()

                left_id = 0
                right_id = 0
                try:
                    left_node = node_map_dict[left_key]
                    left_id = left_node[id_key]
                except KeyError:
                    pass
                try:
                    right_node = node_map_dict[right_key]
                    right_id = right_node[id_key]
                except KeyError:
                    pass

                if left_id == 0 or right_id == 0:
                    fail_cnt += 1
                    continue

                e = self.traversal.V(left_id).as_("from").V(right_id).addE(label).from_("from")

                for propName, propVal in updated_edge.items():
                    if propName in propertySchema:# and propName != "eventTime":
                        # Add property from edge only when it is part of schema
                        data_type = propertySchema[propName]["dataType"]

                        castedPropVal = self.map_value_to_data_type(data_type, propVal)
                        e = e.property(propName, castedPropVal)

                    else:
                        continue

                e = e.next()
                edges_record.append(e.id)

                sucess_cnt += 1

                if counter % self.print_per_record == 0:
                    end_time = datetime.datetime.now()
                    print("Time taken to insert edge no {} is {} ms. Success count {} Failed count {}".format(counter, (end_time-start_time).total_seconds() * 1000, sucess_cnt, fail_cnt))
                    start_time = datetime.datetime.now()

                counter += 1

            edge_ids.append(edges_record)

            record_num += 1

        print("Finished inserting {} edge records. Success {} Fail {}".format(len(self.edges_in_records), sucess_cnt, fail_cnt))

        return edge_ids

    def push(self, node_maps=None):

        if node_maps is not None:
            return self.push_shallow_fast_with_ref(node_maps)
        else:
            return self.push_deep_slow_without_node_ref()

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

    def get_constraints(self, label):
        constraints = self.datamapper["edges"][label]["constraints"]
        uniqueness = constraints["unique"]

        constraintProperties = [x.strip() for x in uniqueness.split("or")]
        return constraintProperties
