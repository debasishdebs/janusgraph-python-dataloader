import json
import pprint
import os


def read_graphSON(path):
    # print(path)
    with open(path, "r", encoding="utf8") as data_file:
        x = data_file.read()

        list1 = x.split('\n')

        if list1[-1] == "":
            del list1[-1]

        graphSON = []

        for i in range(len(list1)):
            try:
                graphSON.append(json.loads(list1[i]))
            except:
                print(x + ' <---blank char')
                pass
            # print(i)

        return graphSON


def get_nodes_from_grpahson(graphson):
    nodes = []
    ids_added = []

    for i in range(len(graphson)):
        record = graphson[i]

        node = dict()
        node["id"] = record["id"]["@value"]
        node["label"] = record["label"]

        properties = record["properties"]

        for propName, propInfo in properties.items():
            val1 = propInfo[0] if len(propInfo) == 1 else propInfo

            if isinstance(val1, list):
                val2 = [x["value"]["@value"] if isinstance(x["value"], dict) else x["value"] for x in val1]
            else:
                val2 = val1["value"]["@value"] if isinstance(val1["value"], dict) else val1["value"]

            node[propName] = val2

        label = record["label"]
        node["image"] = "http://localhost/sstech/Individual-01.svg" if label == "user" else "http://localhost/sstech/IP.svg"
        node["image"] = "./icon.png" if label == "user" else "./ip-icon.png"

        id = node["id"]
        if id not in ids_added:
            nodes.append(node)
            ids_added.append(id)

    return nodes


def get_edges_from_graphson(graphson):
    edges = []
    ids_added = []

    for i in range(len(graphson)):
        record = graphson[i]
        node_id = record["id"]["@value"]

        if 'outE' in record:
            for edgeLabel, edgesForLabel in record['outE'].items():
                for edge_record in edgesForLabel:
                    edge = dict()
                    edge["id"] = edge_record["id"]["@value"]["relationId"]
                    edge['source'] = node_id
                    edge['target'] = edge_record["inV"]["@value"]
                    edge["label"] = edgeLabel

                    properties = edge_record["properties"]

                    for propName, propInfo in properties.items():
                        edge[propName] = propInfo["@value"] if isinstance(propInfo, dict) else propInfo

                    edge_id = edge["id"]

                    edge = filter_props(edge)

                    if edge_id not in ids_added:
                        edges.append(edge)
                        ids_added.append(edge_id)

    return edges


def filter_props(edge, filter = ["source", "target", "label"]):
    f_e = {}
    for k, v in edge.items():
        if k in filter:
            f_e[k] = v
    return f_e


def convert_graphson_to_json(fname):
    full_data_path = os.path.abspath(fname)
    graphson = read_graphSON(full_data_path)
    drive, full_fname = os.path.splitdrive(full_data_path)
    filename, file_extension = os.path.splitext(full_fname)

    nodes = get_nodes_from_grpahson(graphson)
    edges = get_edges_from_graphson(graphson)

    print("Number of nodes: ", len(nodes))
    print("Number of edges: ", len(edges))

    graph = dict()
    graph["nodes"] = nodes
    graph["links"] = edges

    return graph


if __name__ == '__main__':

    files = ["192.168.0.220_hasips.json", "192.168.200.30_events_causedevents_users.json",
             "192.168.200.30_events_hasips_users.json", "192.168.200.30_hasips.json", "192.168.200.30-events_hasips.json"]

    files = ["top10.json"]

    files = ["192.168.0.112_bothE.json", "192.168.0.112_bothE_isEvent.json",
             "192.168.0.112_inE_causedEvent.json", "192.168.0.112_inE_causedEvent_hasIP.json",
             "192.168.0.112_inE_hasIP.json", "192.168.0.112_inE_isEvent.json",
             "192.168.0.112_outE_isEvent.json"]

    files = ["192.168.0.112_inE_causedEvent.json", "192.168.0.112_inE_causedEvent_hasIP.json",
             "192.168.0.112_inE_hasIP.json"]

    files = ["192.168.0.112_outE_isEvent.json"]

    curr_dir = os.getcwd()
    data_dir = "\\Data\\"
    graph_dir = "\\Data\\Graph\\"
    graphson_dir = "\\Data\\GraphSON\\"


    for file in files:

        full_data_path = os.path.abspath(curr_dir + data_dir + file)

        print("Starting to post process ", full_data_path)

        graphson = read_graphSON(full_data_path)
        # graphson = json.load(open(file))

        filename, file_extension = os.path.splitext(file)

        json.dump(graphson, open(os.path.abspath(curr_dir + graphson_dir + "{}.graphson".format(filename)), "w+"), indent=2)

        nodes = get_nodes_from_grpahson(graphson)
        edges = get_edges_from_graphson(graphson)

        print("Number of nodes: ", len(nodes))
        print("Number of edges: ", len(edges))

        graph = dict()
        graph["nodes"] = nodes
        graph["links"] = edges

        json.dump(graph, open(os.path.abspath(curr_dir + graph_dir + "{}.graph.json".format(filename)), "w+"), indent=2)

        print("Converted ", file)
