import json, yaml
import pandas as pd
import requests

# Load nodes data
file_name = 'data_archive/wellness_kg_nodes_v1.3.tsv'
nodes_df = pd.read_csv(file_name, sep='\t')
nodes_df['id_prefix'] = [c.split(':')[0] for c in nodes_df.id]
nodes_df['id_suffix'] = [c.split(':')[1] for c in nodes_df.id]

# Utility functions
def get_op_labels(l1, l2): l = l1 + '-' + l2; return (l, l + 'Rev')
def get_biolink_type(category): return category if not('biolink:' in category) else category.split(':')[-1]

# Map unique curie prefixes to integers
prefix_list = [p for p in nodes_df.id_prefix.drop_duplicates().sort_values()]
prefix_map = dict(zip(prefix_list, range(len(prefix_list))))

# Get unique combinations of ID prefixes and node categories
select_cols = ['category', 'id_prefix']
unique_df = nodes_df[select_cols].drop_duplicates()
unique_df['type'] = [get_biolink_type(c) for c in unique_df.category]
unique_df['type_label'] = [''.join([t, str(prefix_map[p])]) for p, t in zip(unique_df.id_prefix, unique_df.type)]
unique_df = unique_df.sort_values(['type', 'id_prefix'])

ALWAYS_PREFIXED = [
    'RHEA', 'GO', 'CHEBI', 'HP', 'MONDO', 'DOID', 'EFO', 'UBERON', 'MP',
    'CL', 'MGI', 'LOINC', 'KEGG.COMPOUND', 'KEGG.DRUG']

# Generates specification of operations for combinations of type labels
def generate_bte_operations(df, predicate='related_to', provider='Multiomics Provider', sort_by=None, edge_props=[]):
    all_refs = []; all_ops = {}; all_responses = {}
    op_base = '#/components/x-bte-kgs-operations/'
    response_base = '#/components/x-bte-response-mapping/'
    for t1, l1, p1 in zip(df.type, df.type_label, df.id_prefix):
        
        # Generate response mapping
        for i, l in enumerate([l1, l1 + 'Rev']):
            node = 'object' if (i == 0) else 'subject'
            p1_ = p1.replace('.', '_')
            response = {p1_: '.'.join(['hits', node, p1_])}
            for prop in edge_props:
                response.update({prop: '.'.join(['hits', 'predicate', prop])})
            all_responses.update({l: response})
        
        # Generate specification for operations
        for t2, l2, p2 in zip(df.type, df.type_label, df.id_prefix):
            
            # Generate operation labels
            op_labels = get_op_labels(l1, l2)
            current_refs = [{'$ref': op_base + l} for l in op_labels]
            all_refs = all_refs + current_refs
            
            # Generate specifications for operations
            for i, l in enumerate(op_labels):
                node_order = ['subject', 'object'] if (i == 0) else ['object', 'subject']
                response_label = l.split('-')[-1]
                
                # Specification for operations
                curie_id = '"{inputs[0]}"' if (p1 in ALWAYS_PREFIXED) else '\:'.join([p1, '{inputs[0]}'])
                params = {
                    'fields': ','.join([node_order[1], 'predicate']),
                    'q': '{0}.id:{2} AND {1}.type:{3}'.format(*node_order, curie_id, t2),
                    'size': 1000}
                if not(sort_by is None):
                    params.update({'sort': sort_by})
                
                op = {
                    'inputs': [{'id': p1, 'semantic': t1}],
                    'outputs': [{'id': p2, 'semantic': t2}],
                    'parameters': params,
                    'predicate': predicate,
                    'response_mapping': {'$ref': response_base + response_label},
                    'source': provider,
                    'supportBatch': False
                }
                all_ops.update({l: [op]})
    
    return all_refs, all_ops, all_responses

# Specify arguments
sort_by = 'predicate.Bonferroni_pval'
predicate = 'correlated_with'
edge_props = ['N', 'Bonferroni_pval', 'rho', 'provenance']
provider = 'Multiomics Provider: Wellness'

# Get operations references, operations, and responses
refs, ops, responses = \
    generate_bte_operations(
        unique_df, predicate=predicate, sort_by=sort_by, edge_props=edge_props, provider=provider)

# Final specifications
refs = {'x-bte-kgs-operations': refs}
ops = {'x-bte-kgs-operations': ops}
responses = {'x-bte-response-mapping': responses}

# Utility function to write yaml files
def to_yaml(json_doc, output_file):
    with open(output_file, 'w') as file:
        docs = yaml.dump(json_doc, file)

# Write to files
output_file = "x_bte_operations_refs.yaml"
to_yaml(refs, output_file)
output_file = "x_bte_operations.yaml"
to_yaml(ops, output_file)
output_file = "x_bte_response_mappings.yaml"
to_yaml(responses, output_file)
