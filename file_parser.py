import csv
import os
import json


def load_data(data_folder):
    edges_file_path = os.path.join(data_folder, "wellness_kg_edges_v1.3.tsv")
    nodes_file_path = os.path.join(data_folder, "wellness_kg_nodes_v1.3.tsv")
    nodes_f = open(nodes_file_path)
    edges_f = open(edges_file_path)
    nodes_data = csv.reader(nodes_f, delimiter="\t")
    edges_data = csv.reader(edges_f, delimiter="\t")
    next(nodes_data)
    id_name_mapping = {}
    id_type_mapping = {}
    for line in nodes_data:
        id_name_mapping[line[0]] = line[1]
        id_type_mapping[line[0]] = line[2].split(':')[-1] if line[2].startswith("biolink:") else line[2]
    next(edges_data)
    for line in edges_data:
        if line[0] and line[1] and line[0].split(':')[0] and line[2].split(':')[0]:
            # Specify properties for subject
            subject = {
                "id": line[0],
                line[0].split(':')[0].replace('.', '_'): line[0],
                "name": id_name_mapping[line[0]],
                "type": id_type_mapping[line[0]]
            }

            # Specify properties for object
            object_ = {
                "id": line[2],
                line[2].split(':')[0].replace('.', '_'): line[2],
                "name": id_name_mapping[line[2]],
                "type": id_type_mapping[line[2]]
            }

            # Specify properties for predicate
            predicate = {
                "type": line[1].split(':')[-1] if line[1].startswith("biolink:") else line[1],
                "relation": line[3],
                "provided_by": "Multiomics Provider: Wellness",
                "category": line[9].split(':')[-1] if line[9].startswith("biolink:") else line[9],
                "N": int(float(line[4])),
                "rho": int(float(line[5])*1e5)/1e5,
                "Bonferroni_pval": float(line[6])
            }

            # Yield subject, predicate, and object properties
            yield {
                "_id": '-'.join([line[0], line[1], line[2], str(int(float(line[4]))), str(line[5])[:6], str(line[6])]),
                "subject": subject,
                "predicate": predicate,
                "object": object_
            }
