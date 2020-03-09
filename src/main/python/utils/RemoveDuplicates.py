import datetime as dt
from dateutil import parser


class RemoveDuplicates(object):
    def __init__(self, datamapper):
        self.nodes = []
        self.edges = []
        self.node_mapper = datamapper["nodes"]
        self.edge_mapper = datamapper["edges"]
        pass

    def fit(self, nodes, edges):
        self.nodes = nodes
        self.edges = edges
        return self

    def deduplicate_nodes_and_edges(self):
        node_ids = []
        deduplicated_nodes = []

        unique_edge_time_identifies = {
            "causedEvent": "eventTime",
            "isEvent": "eventTime",
            "hasIP": "usedTime",
            "assignedInternalIP": "assignedTime",
            "runningProcess": "eventTime",
            "sentEmail": "eventTime",
            "receivedEmail": "eventTime",
            "userInteracted": "eventTime",
            "processCommunicated": "eventTime",
            "createdProcess": "eventTime"
        }

        # Logic to generate unique ID for an edge is
        # (leftID + edgeLabel + unique_identifier(eventTime/assignedTime etc) + rightID)
        edge_ids = []
        # deduplicated_edge_dict = dict()
        deduplicated_edge_dbl_list = []

        for i in range(len(self.nodes)):
            nodes_in_record = self.nodes[i]

            for node in nodes_in_record:

                label = node["label"]
                cont = self.node_mapper[label]["constraints"]["unique"].split("or")
                id_val = ""
                for c in cont:
                    if c.strip() in node and node[c.strip()] != "":
                        id_val += node[c.strip()]
                id_val = id_val.strip()

                if id_val not in node_ids:
                    node_ids.append(id_val)
                    deduplicated_nodes.append([node])

            edges_in_record = self.edges[i]

            for edge in edges_in_record:
                left = edge["left"]["propVal"]
                right = edge["right"]["propVal"]
                label = edge["label"]

                try:
                    unique_val = parser.parse(edge[unique_edge_time_identifies[label]])

                    # Generate normalized value of eventTime without secods
                    unique_val_normalised = unique_val - dt.timedelta(seconds=unique_val.second)
                except TypeError:
                    unique_val = edge[unique_edge_time_identifies[label]]

                    # Generate normalized value of eventTime without secods
                    unique_val_normalised = unique_val - dt.timedelta(seconds=unique_val.second)
                except KeyError:
                    # When mapped key is absent
                    unique_val_normalised = ""

                unique_id_gen = str(left + label + str(unique_val_normalised) + right)

                if unique_id_gen not in edge_ids:
                    edge_ids.append(unique_id_gen)
                    edge["edge_id"] = unique_id_gen
                    # deduplicated_edge_dict[unique_id_gen] = [edge]
                    deduplicated_edge_dbl_list.append([edge])

                else:
                    # First retrive the edge from list of edges (deduplicated one)
                    e_idx = edge_ids.index(unique_id_gen)
                    edge_already_added = deduplicated_edge_dbl_list[e_idx][0]
                    # edge_already_added = deduplicated_edge_dict[unique_id_gen][0]

                    cntr = int(edge_already_added["counter"])
                    cntr += 1
                    edge_already_added["counter"] = cntr

                    edge_ids.pop(e_idx)
                    deduplicated_edge_dbl_list.pop(e_idx)

                    edge_ids.append(unique_id_gen)
                    deduplicated_edge_dbl_list.append([edge_already_added])
                    # deduplicated_edge_dict.pop(unique_id_gen)
                    # deduplicated_edge_dict[unique_id_gen] = [edge_already_added]

            if i % 100000 == 0:
                print("Deduplicated nodes & edges in {} records".format(i))

        return deduplicated_nodes, deduplicated_edge_dbl_list

    def deduplicate_nodes_in_record(self, nodes_in_record):

        return

    def deduplicate_nodes(self):
        node_ids = []
        deduplicated_nodes = []

        i = 0
        for nodes_in_record in self.nodes:
            for node in nodes_in_record:

                label = node["label"]
                cont = self.node_mapper[label]["constraints"]["unique"].split("or")
                id_val = ""
                for c in cont:
                    if c.strip() in node and node[c.strip()] != "":
                        id_val += node[c.strip()]
                id_val = id_val.strip()

                if id_val not in node_ids:
                    node_ids.append(id_val)
                    deduplicated_nodes.append([node])

                if i % 5000000 == 0:
                    print("Deduplicated {} nodes".format(i))

                i += 1

        return deduplicated_nodes

    def deduplicate_edges(self):
        # This will also take care of aggregating all events with same eventTime, source and target,
        # and update their counter prop

        unique_edge_time_identifies = {
            "causedEvent": "eventTime",
            "isEvent": "eventTime",
            "hasIP": "usedTime",
            "assignedInternalIP": "assignedTime",
            "runningProcess": "eventTime",
            "sentEmail": "eventTime",
            "receivedEmail": "eventTime",
            "userInteracted": "eventTime",
            "processCommunicated": "eventTime",
            "createdProcess": "eventTime"
        }

        # Logic to generate unique ID for an edge is (leftID + edgeLabel + unique_identifier + rightID)
        edge_ids = []
        # deduplicated_edge_dict = dict()
        deduplicated_edge_dbl_list = [[], []]

        i = 0
        for edges_in_record in self.edges:
            for edge in edges_in_record:
                left = edge["left"]["propVal"]
                right = edge["right"]["propVal"]
                label = edge["label"]

                try:
                    unique_val = parser.parse(edge[unique_edge_time_identifies[label]])
                except TypeError:
                    unique_val = edge[unique_edge_time_identifies[label]]

                # Generate normalized value of eventTime without secods
                unique_val_normalised = unique_val - dt.timedelta(seconds=unique_val.second)

                unique_id_gen = str(left + label + str(unique_val_normalised) + right)

                if unique_id_gen not in edge_ids:
                    edge_ids.append(unique_id_gen)
                    edge["edge_id"] = unique_id_gen
                    # deduplicated_edge_dict[unique_id_gen] = [edge]

                    deduplicated_edge_dbl_list[0].append(unique_id_gen)
                    deduplicated_edge_dbl_list[1].append([edge])
                else:
                    # First retrive the edge from list of edges (deduplicated one)
                    # edge_already_added = deduplicated_edge_dict[unique_id_gen][0]
                    e_idx = deduplicated_edge_dbl_list[0].index(unique_id_gen)
                    edge_already_added = deduplicated_edge_dbl_list[1][e_idx][0]

                    cntr = int(edge_already_added["counter"])
                    cntr += 1
                    edge_already_added["counter"] = cntr

                    deduplicated_edge_dbl_list[0].pop(e_idx)
                    deduplicated_edge_dbl_list[1].pop(e_idx)

                    deduplicated_edge_dbl_list[0].append(unique_id_gen)
                    deduplicated_edge_dbl_list[1].append([edge_already_added])

                    # deduplicated_edge_dict.pop(unique_id_gen)
                    # deduplicated_edge_dict[unique_id_gen] = [edge_already_added]

            if i % 5000000 == 0:
                print("Deduplicated {} edges".format(i))

            i += 1

        return deduplicated_edge_dbl_list[1]
