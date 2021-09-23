import csv
import os


def load_data(data_folder):
    edges_file_path = os.path.join(data_folder, "wellness_kg_edges_v1.5.1.tsv")
    nodes_file_path = os.path.join(data_folder, "wellness_kg_nodes_v1.5.1.tsv")
    nodes_f = open(nodes_file_path)
    edges_f = open(edges_file_path)
    nodes_data = csv.reader(nodes_f, delimiter="\t")
    edges_data = csv.reader(edges_f, delimiter="\t")
    next(nodes_data)
    id_name_mapping = {}
    id_type_mapping = {}

    for line in nodes_data:
        id_name_mapping[line[0]] = line[1]
        id_type_mapping[line[0]] = line[2].split(
            ':')[-1] if line[2].startswith("biolink:") else line[2]

    next(edges_data)
    for line in edges_data:
        if line[0] and line[1] and line[0].split(':')[0] and line[2].split(':')[0]:
            # Specify properties for subject
            subject = {
                "id": line[0],
                line[0].split(':')[0].replace('.', '_'): line[0].split(":")[1],
                "name": id_name_mapping[line[0]],
                "type": id_type_mapping[line[0]]
            }

            # Specify properties for object
            object_ = {
                "id": line[2],
                line[2].split(':')[0].replace('.', '_'): line[2].split(":")[1],
                "name": id_name_mapping[line[2]],
                "type": id_type_mapping[line[2]]
            }

            # Specify properties for predicate
            predicate = {
                "type": line[1].split(':')[-1] if line[1].startswith("biolink:") else line[1],
                "relation": line[3],
                "category": line[6].split(':')[-1] if line[6].startswith("biolink:") else line[6],
                "provided_by": "Multiomics Provider: Wellness",
                "provenance": "https://github.com/NCATSTranslator/Translator-All/wiki/Wellness-KP",
                "N": int(float(line[7])),
                "type_of_relationship": line[9],
                "strength_of_relationship": round(float(line[10]),4),
                "bonferroni_pval": round(float(line[13]),4)
            }

            # Add weighted pvalue, if available
            weighted_pvalue = None if (
                line[8] == '') else round(float(line[8]),4)
            if not(weighted_pvalue is None):
                predicate.update({"weighted_pvalue": weighted_pvalue})

            # Add qualifier, if available
            qualifier = None if (line[11] == '') else line[11]
            qualifier_value = None if (line[12] == '') else line[12]
            if not(qualifier is None):
                predicate.update(
                    {'qualifier': qualifier, 'qualifier_value': qualifier_value})

            # Yield subject, predicate, and object properties
            data = {
                "_id": '-'.join([line[0], line[1], line[2], str(int(float(line[7]))), str(line[10])[:6], str(line[13])]),
                "subject": subject,
                "predicate": predicate,
                "object": object_
            }

            yield data
