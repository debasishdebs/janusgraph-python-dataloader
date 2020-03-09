import json
import sys, os
sys.path.append(os.path.abspath(os.getcwd() + "\\..\\"))


class DataExtractor(object):
    def __init__(self, schema):
        """

        Args:
            schema (dict):
        """

        self.nodes = []
        self.edges = []
        self.records = []
        self.datamapper = dict()
        self.schema = schema

        self.node_labels = list(set([x["label"] for x in self.schema["nodes"]]))
        self.edge_labels = list(set([x["label"] for x in self.schema["edges"]]))

        pass

    def fit(self, datamapper, records):
        self.datamapper = datamapper
        self.records = records
        return self

    def get_nodes_and_edges(self):
        nodes = []
        edges = []

        for record in self.records:
            entities = self.extract_entities(record)
            nodes.append(entities)

            relationships = self.extract_relationships(record)
            edges.append(relationships)

        return nodes, edges

    def extract_relationships(self, record):
        relationships = []

        for label in self.edge_labels:
            maps_for_label = self.datamapper["edges"][label]["maps"]
            constraints = self.datamapper["edges"][label]["constraints"]

            srcRelationship = dict()
            dstRelationship = dict()
            relationship = dict()
            dst_relationships = []

            relations_source_dst_info = self.extract_left_right_info_for_generic_edges(constraints, record)

            for src_dst_info in relations_source_dst_info:
                left = src_dst_info["left"]
                right = src_dst_info["right"]

                relationship["label"] = label
                relationship["left"] = left
                relationship["right"] = right

                for propName, propMap in maps_for_label.items():
                    relationship[propName] = self.get_property_from_map(record, propMap)

                blank = False
                if "left" in relationship and "right" in relationship:
                    if relationship["left"]["propVal"] in ["", "-", "NA"] or relationship["right"]["propVal"] in ["", "-", "NA"]:
                        # Invalid edge, when either's source of destination info isn't available
                        blank = True
                else:
                    blank = True

                if not blank:
                    relationships.append(relationship)
            #
            # if label == "hasIP":
            #     srcInfo, dstInfo = self.extract_left_right_info_for_hasIP(constraints, record)
            #
            #     srcRelationship["left"] = srcInfo["left"]
            #     srcRelationship["right"] = srcInfo["right"]
            #
            #     dstRelationship["left"] = dstInfo["left"]
            #     dstRelationship["right"] = dstInfo["right"]
            #
            #     srcRelationship["label"] = label
            #     dstRelationship["label"] = label
            #
            #     for propName, propMap in maps_for_label.items():
            #         srcRelationship[propName] = self.get_property_from_map(record, propMap)
            #         dstRelationship[propName] = self.get_property_from_map(record, propMap)
            #
            #     if dstRelationship["left"]["propName"] == "user.userName" and ";" in dstRelationship["left"]["propVal"]:
            #         values = dstRelationship["left"]["propVal"].split(";")
            #
            #         for val in values:
            #             tmp_rel = dict()
            #
            #             tmp_rel.update({k: v for k, v in dstRelationship.items()})
            #
            #             tmp_rel["left"]["propVal"] = val
            #
            #             dst_relationships.append(tmp_rel)
            #
            # else:
            #     left, right = self.extract_relationship_left_right_info(constraints, record)
            #
            #     relationship["label"] = label
            #     relationship["left"] = left
            #     relationship["right"] = right
            #
            #     for propName, propMap in maps_for_label.items():
            #         relationship[propName] = self.get_property_from_map(record, propMap)
            #
            # if len(dst_relationships) == 0:
            #     relationships_in_record = [relationship, srcRelationship, dstRelationship]
            # else:
            #     relationships_in_record = [relationship, srcRelationship, *dst_relationships]
            #
            # for r in relationships_in_record:
            #     blank = False
            #
            #     if "left" in r:
            #         if r["left"]["propVal"] in ["", "-"] or r["right"]["propVal"] in ["", "-"]:
            #             # Invalid edge, when either's source of destination info isn't available
            #             blank = True
            #     else:
            #         blank = True
            #
            #     if not blank:
            #         relationships.append(r)

        return relationships

    def extract_entities(self, record):
        entities = []

        for label in self.node_labels:

            maps_for_label = self.datamapper["nodes"][label]["maps"]

            src_entity = dict()
            dst_entity = dict()
            entity = dict()
            dst_entities = []

            if label in ["IP", "user"]:
                # We will now extract 2 IPs and 2 Users.
                src_entity["label"] = label
                dst_entity["label"] = label

                for propName, propMap in maps_for_label.items():
                    if "analyze" not in propMap:
                        if "+" in propMap:
                            appenders = propMap.split("+")
                            propVal = ""
                            for appender in appenders:
                                propVal += self.get_property_from_map(record, appender)

                            srcVal = propVal
                            dstVal = propVal

                        else:
                            propVal = self.get_property_from_map(record, propMap)
                            srcVal = propVal
                            dstVal = propVal

                    else:
                        if "+" in propMap or "|" in propMap:
                            raise NotImplementedError("Currently not implemented mixed mapper containing "
                                                      "'analyze' and '|' and '+'")
                        else:
                            func_map = propMap.split("analyze")[1][1:]
                            func_name = self.datamapper["analyze"][func_map]
                            module_name = func_name.split("&")[0].split("=")[1]
                            package_name = func_name.split("&")[1].split("=")[1]
                            func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

                            srcVal, dstVal = func(record)

                    # We know that destination val can be ";" separated corresponding to multiple users
                    src_entity[propName] = srcVal
                    dst_entity[propName] = dstVal

                if label == "user" and ";" in dst_entity["userName"]:
                    values = dst_entity["userName"].split(";")

                    for val in values:
                        tmp_d = dict()
                        tmp_d.update({k: v for k, v in dst_entity.items()})

                        tmp_d["userName"] = val

                        dst_entities.append(tmp_d)

            else:
                entity["label"] = label

                for propName, propMap in maps_for_label.items():
                    if "analyze" not in propMap:
                        if "+" in propMap:
                            appenders = propMap.split("+")
                            propVal = ""
                            for appender in appenders:
                                propVal += self.get_property_from_map(record, appender)

                        else:
                            propVal = self.get_property_from_map(record, propMap)

                    else:
                        if "+" in propMap or "|" in propMap:
                            raise NotImplementedError("Currently not implemented mixed mapper containing "
                                                      "'analyze' and '|' and '+'")
                        else:
                            func_map = propMap.split("analyze")[1][1:]
                            func_name = self.datamapper["analyze"][func_map]
                            module_name = func_name.split("&")[0].split("=")[1]
                            package_name = func_name.split("&")[1].split("=")[1]
                            func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

                            propVal = func(record)

                    entity[propName] = propVal

            if len(dst_entities) == 0:
                entities_in_record = [entity, src_entity, dst_entity]
            else:
                entities_in_record = [entity, src_entity, *dst_entities]

            for entity in entities_in_record:
                blank = True
                for k, v in entity.items():
                    if k in ["label", "node_label", "dataSourceName"]:
                        pass
                    else:
                        if v not in ["", "-"]:
                            blank = False
                if not blank:
                    entities.append(entity)

        return entities

    def get_property_from_map(self, record, propMap):
        """

        Args:
            record (dict):
            propMap (str):

        Returns:

        """

        if "|" in propMap:
            maps = propMap.split("|")
            m1 = maps[0]
            m2 = maps[1]
            if m1 in record and m2 in record:
                if record[m1] == record[m2]:
                    propVal = record[m1]
                elif record[m1] in ["-", ""]:
                    propVal = record[m2]
                elif record[m2] in ["-", ""]:
                    propVal = record[m1]
                else:
                    # Give preference to 1st property
                    propVal = record[m1]
            else:
                if m1 in record and m2 not in record:
                    propVal = record[m1]
                elif m2 in record and m1 not in record:
                    propVal = record[m2]
                else:
                    print("Both {} and {} are missing in record. Defaulting to NA".format(m1, m2))
                    propVal = "NA"

        elif propMap.isalpha():
            if propMap in record:
                propVal = record[propMap]
            else:
                print("Couldn't find corresponding key {} in record. Defaulting to ''".format(propMap))
                propVal = ""

        elif propMap in record:
            propVal = record[propMap]

        else:
            if "default" in propMap:
                # When default is provided. It is of format default=something in datamapper
                val = propMap.split("default=")[1]
                propVal = val
            else:
                propVal = propMap

        return propVal

    def extract_left_right_info_for_generic_edges(self, constraints, record):
        leftInfo = constraints["left"]
        rightInfo = constraints["right"]

        relations = [dict]

        leftPropName = leftInfo.split("(")[0]
        leftPropRecRef = leftInfo.split("(")[1][:-1]

        rightPropName = rightInfo.split("(")[0]
        rightPropRecRef = rightInfo.split("(")[1][:-1]

        if "analyze" in leftPropRecRef and "analyze" in rightPropRecRef:
            if ("+" in leftPropRecRef or "|" in leftPropRecRef) or ("+" in rightPropRecRef or "|" in rightPropRecRef):
                raise NotImplementedError("Currently not implemented mixed mapper containing "
                                          "'analyze' and '|' and '+'. Got mixed mapping in either left or right")

            func_map = leftPropRecRef.split("analyze")[1][1:]
            func_name = self.datamapper["analyze"][func_map]
            module_name = func_name.split("&")[0].split("=")[1]
            package_name = func_name.split("&")[1].split("=")[1]
            func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

            leftValues = func(record)

            func_map = rightPropRecRef.split("analyze")[1][1:]
            func_name = self.datamapper["analyze"][func_map]
            module_name = func_name.split("&")[0].split("=")[1]
            package_name = func_name.split("&")[1].split("=")[1]
            func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

            rightValues = func(record)

            if len(leftValues) != len(rightValues):
                # One-Many mapping when one can be either left or right
                if len(leftValues) != 1 and len(rightValues) != 1:
                    raise NotImplementedError("Expecting the length of output from custom functions of same length. "
                                              "If inequal atleast one should be of length 1 so that either One-Many or "
                                              "One-One mapping can be done for left and right")

                # One-Many relationships when one is of length 1 and other of X
                if len(leftValues) == 1:
                    for val in rightValues:
                        rel = {"left": {
                            "propName": leftPropName,
                            "propVal": leftValues[0]
                        }, "right": {
                            "propName": rightPropName,
                            "propVal": val
                        }}
                        relations.append(rel)
                else:
                    # When there are multiple left but single right
                    for val in leftValues:
                        rel = {"left": {
                            "propName": leftPropName,
                            "propVal": val
                        }, "right": {
                            "propName": rightPropName,
                            "propVal": rightValues[0]
                        }}
                        relations.append(rel)

            else:
                # One-One mapping when leftValues len = rightValue len
                for i in range(len(leftValues)):
                    leftVal = leftValues[i]
                    rightVal = rightValues[i]

                    rel = {"left": {
                        "propName": leftPropName,
                        "propVal": leftVal
                    }, "right": {
                        "propName": rightPropName,
                        "propVal": rightVal
                    }}
                    relations.append(rel)

        elif "analyze" in leftPropRecRef and "analyze" not in rightPropRecRef:
            func_map = leftPropRecRef.split("analyze")[1][1:]
            func_name = self.datamapper["analyze"][func_map]
            module_name = func_name.split("&")[0].split("=")[1]
            package_name = func_name.split("&")[1].split("=")[1]
            func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

            leftValues = func(record)

            rightVal = self.get_property_from_map(record, rightPropRecRef)

            for leftVal in leftValues:
                rel = {"left": {
                    "propName": leftPropName,
                    "propVal": leftVal
                }, "right": {
                    "propName": rightPropName,
                    "propVal": rightVal
                }}
                relations.append(rel)

        elif "analyze" not in leftPropRecRef and "analyze" in rightPropRecRef:
            func_map = rightPropRecRef.split("analyze")[1][1:]
            func_name = self.datamapper["analyze"][func_map]
            module_name = func_name.split("&")[0].split("=")[1]
            package_name = func_name.split("&")[1].split("=")[1]
            func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

            rightValues = func(record)

            leftVal = self.get_property_from_map(record, leftPropRecRef)

            for rightVal in rightValues:
                rel = {"left": {
                    "propName": leftPropName,
                    "propVal": leftVal
                }, "right": {
                    "propName": rightPropName,
                    "propVal": rightVal
                }}
                relations.append(rel)

        else:
            leftVal = self.get_property_from_map(record, leftPropRecRef)
            rightVal = self.get_property_from_map(record, rightPropRecRef)

            rel = {"left": {
                "propName": leftPropName,
                "propVal": leftVal
            }, "right": {
                "propName": rightPropName,
                "propVal": rightVal
            }}
            relations.append(rel)

        return relations

    def extract_left_right_info_for_hasIP(self, constraints, record):
        # This can work with "analyze" reference in edge datamapper
        leftInfo = constraints["left"]
        rightInfo = constraints["right"]

        srcLeft = dict()
        srcRight = dict()
        dstLeft = dict()
        dstRight = dict()

        # Left Node info, i.e. in vertex
        propName = leftInfo.split("(")[0]
        propRecRef = leftInfo.split("(")[1][:-1]
        srcLeft["propName"] = propName
        dstLeft["propName"] = propName

        if "analyze" not in propRecRef:
            if "+" in propRecRef:
                appenders = propRecRef.split("+")
                propVal = ""
                for appender in appenders:
                    propVal += self.get_property_from_map(record, appender)

                srcVal = propVal
                dstVal = propVal

            else:
                propVal = self.get_property_from_map(record, propRecRef)
                srcVal = propVal
                dstVal = propVal

        else:
            if "+" in propRecRef or "|" in propRecRef:
                raise NotImplementedError("Currently not implemented mixed mapper containing "
                                          "'analyze' and '|' and '+'")
            else:
                func_map = propRecRef.split("analyze")[1][1:]
                func_name = self.datamapper["analyze"][func_map]
                module_name = func_name.split("&")[0].split("=")[1]
                package_name = func_name.split("&")[1].split("=")[1]
                func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

                srcVal, dstVal = func(record)

        srcLeft["propVal"] = srcVal
        dstLeft["propVal"] = dstVal

        # Right Node info, i.e. in vertex
        propName = rightInfo.split("(")[0]
        propRecRef = rightInfo.split("(")[1][:-1]
        srcRight["propName"] = propName
        dstRight["propName"] = propName

        if "analyze" not in propRecRef:
            if "+" in propRecRef:
                appenders = propRecRef.split("+")
                propVal = ""
                for appender in appenders:
                    propVal += self.get_property_from_map(record, appender)

                srcVal = propVal
                dstVal = propVal

            else:
                propVal = self.get_property_from_map(record, propRecRef)
                srcVal = propVal
                dstVal = propVal

        else:
            if "+" in propRecRef or "|" in propRecRef:
                raise NotImplementedError("Currently not implemented mixed mapper containing "
                                          "'analyze' and '|' and '+'")
            else:
                func_map = propRecRef.split("analyze")[1][1:]
                func_name = self.datamapper["analyze"][func_map]
                module_name = func_name.split("&")[0].split("=")[1]
                package_name = func_name.split("&")[1].split("=")[1]
                func = getattr(__import__(module_name, fromlist=[package_name]), package_name)

                srcVal, dstVal = func(record)

        srcRight["propVal"] = srcVal
        dstRight["propVal"] = dstVal

        return {"left": srcLeft, "right": srcRight}, {"left": dstLeft, "right": dstRight}

    def extract_relationship_left_right_info(self, constraints, record):
        # EXPECTS: No "analyze" based reference present in edge data mapper
        leftInfo = constraints["left"]
        rightInfo = constraints["right"]
        left = dict()
        right = dict()

        # Left Node info, i.e. in vertex
        propName = leftInfo.split("(")[0]
        propRecRef = leftInfo.split("(")[1][:-1]

        left["propName"] = propName
        if "+" in propRecRef:
            appenders = propRecRef.split("+")
            propVal = ""
            for appender in appenders:
                propVal += self.get_property_from_map(record, appender)

        else:
            propVal = self.get_property_from_map(record, propRecRef)

        if isinstance(propVal, str) and "$" in propVal:
            propVal = propVal.replace("$", "")

        left["propVal"] = propVal

        # Right Node info, i.e. out vertex
        propName = rightInfo.split("(")[0]
        propRecRef = rightInfo.split("(")[1][:-1]

        right["propName"] = propName
        if "+" in propRecRef:
            appenders = propRecRef.split("+")
            propVal = ""
            for appender in appenders:
                propVal += self.get_property_from_map(record, appender)

        else:
            propVal = self.get_property_from_map(record, propRecRef)

        if isinstance(propVal, str) and "$" in propVal:
            propVal = propVal.replace("$", "")

        right["propVal"] = propVal

        return left, right


if __name__ == '__main__':

    records_files = ["../../resources/data/msexchange_sample.records"]

    for file in records_files:
        records_dirname, fname = os.path.split(file)

        out_fname = os.path.splitext(fname)[0] + ".graph"

        records_file = file
        schema_file = "../../resources/schema/schema_msexchange_sample.json"
        datamapper_file = "../../resources/schema/datamapper_msexchange_sample.json"

        records = json.load(open(records_file))
        records_sample = records[:int(0.05*len(records))]
        schema = json.load(open(schema_file))
        datamapper = json.load(open(datamapper_file))

        print("Read in ", records_file)

        extractor = DataExtractor(schema)
        extractor.fit(datamapper, records)
        nodes, edges = extractor.get_nodes_and_edges()

        print("Extracted Graph from logs for ", file)
        print("Total nodes: ", sum([len(x) for x in nodes]))
        print("Total edges: ", sum([len(l) for l in edges]))

        graph = dict()
        graph["nodes"] = nodes
        graph["edges"] = edges

        print("Dumping total graph for ", file)
        json.dump(graph,
                  open(os.path.abspath(records_dirname + "/" + out_fname), "w+"), indent=2)

        print("Extracted {} to {}".format(file, os.path.abspath(records_dirname + "/" + out_fname)))
        print(100*"=")
