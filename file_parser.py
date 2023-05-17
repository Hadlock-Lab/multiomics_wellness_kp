import csv
import os
import json

attribute_source = "infores:biothings-multiomics-wellness"
attribute_data_source = "infores:isb-wellness"

correlation_statistic = {
    "Ridge regression coefficient": {
        "attribute_type_id": "NCIT:C53237", # Regression Method -- http://purl.obolibrary.org/obo/NCIT_C53237
        "description": "Ridge regression coefficient was used to compute the value for the association",
        "value": "ENM:8000094",
        #"value_type_id": "biolink:id"
    },
    "Spearman Correlation": {
        "attribute_type_id": "NCIT:C53236", # Correlation Test -- http://purl.obolibrary.org/obo/NCIT_C53236
        "description": "Spearman Correlation Test was used to compute the p-value for the association",
        "value": "NCIT:C53249", # Spearman Correlation Test -- http://purl.obolibrary.org/obo/NCIT_C53249
        #"value_type_id": "biolink:id"
    }
}



def load_data(data_folder):
    edges_file_path = os.path.join(data_folder, "wellness_kg_edges_v1.7.tsv")
    nodes_file_path = os.path.join(data_folder, "wellness_kg_nodes_v1.7.tsv")
    nodes_f = open(nodes_file_path)
    edges_f = open(edges_file_path)
    nodes_data = csv.reader(nodes_f, delimiter="\t")
    edges_data = csv.reader(edges_f, delimiter="\t")
    next(nodes_data)
    id_name_mapping = {}
    id_type_mapping = {}

    for line in nodes_data:
        id_name_mapping[line[0]] = line[1]
        id_type_mapping[line[0]] = line[2]

    next(edges_data)
    for line in edges_data:
        if line[0] and line[1] and line[0].split(':')[0] and line[2].split(':')[0]:

            prefix = line[0].split(':')[0]
            subject = {
                "id": line[0],
                prefix: line[0],
                "name": id_name_mapping[line[0]],
                "type": id_type_mapping[line[0]]
            }

            prefix = line[2].split(':')[0]
            object_ = {
                "id": line[2],
                prefix: line[2],
                "name": id_name_mapping[line[2]],
                "type": id_type_mapping[line[2]]
            }


            # properties for predicate/association
            edge_attributes = []

            # Type_of_relationship
            # could be Ridge regression or Spearman
            if line[8] in correlation_statistic:
                edge_attributes.append(correlation_statistic[line[8]])
            else:
                #print(line)
                raise Exception(f"Column 8 has unexpected value for type of statistic: {line[8]}")

            # TRAPI 1.3 style source
            edge_attributes.append(
                {
                    "attribute_type_id": "biolink:primary_knowledge_source",
                    "value": attribute_source
                }
            )

            # strength_of_relationship
            edge_attributes.append(
                {
                    "attribute_type_id": "STATO:0000085", # http://purl.obolibrary.org/obo/STATO_0000085
                    "description": "Effect size estimate",
                    "value": float(line[9]),
                    #"value_type_id": "biolink:XXX" # ???
                }
            )

            # relation
            edge_attributes.append(
                {
                    "attribute_type_id": line[6],
                    "description": "Predicate id",
                    "value": line[3],
                    #"value_type_id": "biolink:id" # ???
                }
            )

            # N
            edge_attributes.append(
                {
                    "attribute_type_id": "GECKO:0000106", # http://purl.obolibrary.org/obo/GECKO_0000106
                    "description": "Sample size used to compute the correlation", # ???
                    "value": int(float(line[7]))
                }
            )

            # bonferroni_pval
            edge_attributes.append(
                {
                    "attribute_type_id": "NCIT:C61594",
                    "description": "Bonferroni pvalue",
                    "value": float(line[13])
                }
            )


            # Add qualifier, if available
            domain = None if (line[10] == '' or line[10] == 'nan') else line[10]
            qualifier = None if (line[11] == '' or line[11] == 'nan') else line[11]
            qualifier_value = None if (line[12] == '' or line[12] == 'nan') else line[12]
            if not(qualifier is None):
                edge_attributes.append(
                    {
                        "attribute_type_id": qualifier,
                        "description": domain,
                        "value": qualifier_value,
                        #"value_type_id": "XXX ???" # ???
                    }
                )

            # sources - TRAPI 1.4 style
            edge_sources = [
                {
                    "resource_id": attribute_source,
                    "resource_role": "primary_knowledge_source"
                },
                {
                    "resource_id": attribute_data_source,
                    "resource_role": "supporting_data_source"
                }
            ]


            association = {
                "label": line[1],
                "attributes": edge_attributes,
                "sources": edge_sources
            }

            # Yield subject, predicate, and object properties
            data = {
                "_id": '-'.join(['WKP', line[0], line[1], line[2], domain, qualifier_value]).replace(" ","_"),
                "subject": subject,
                "association": association,
                "object": object_
            }
            yield data

        else:
            print(f"Cannot find prefix for {line} !")



def main():
    testing = False #True
    done = 0
    gen = load_data('test')
    while not testing or done < 10:
        try: entry = next(gen)
        except: break
        else:
            print(json.dumps(entry, sort_keys=True, indent=2))
            done = done + 1


if __name__ == '__main__':
    main()
