import json
from collections import Counter, defaultdict
import sys

sample_file = "/home/ubuntu/spoke/data/spokeV6_edge_tail_sample.jsonl"
edge_counts = Counter()
duplicate_examples = defaultdict(list)
total_lines = 0
processed_relationships = 0
skipped_lines = 0
max_examples_per_group = 3 
max_example_groups = 5     

print(f"Analyzing file: {sample_file}")

try:
    with open(sample_file, 'r') as f:
        for line_num, line in enumerate(f):
            total_lines += 1
            line_content = line.strip()
            if not line_content:
                skipped_lines += 1
                continue

            try:
                data = json.loads(line_content)

                if data.get('type') == 'relationship':
                    start_node_obj = data.get('start')
                    end_node_obj = data.get('end')
                    rel_type_label = data.get('label') # Use 'label' based on validate_jsonl.py

                    start_node_id = start_node_obj.get('id') if isinstance(start_node_obj, dict) else None
                    end_node_id = end_node_obj.get('id') if isinstance(end_node_obj, dict) else None

                    valid_key = True
                    if not isinstance(start_node_id, str):
                        valid_key = False
                    if not isinstance(end_node_id, str):
                        valid_key = False
                    if not isinstance(rel_type_label, str):
                        valid_key = False

                    if valid_key:
                        edge_key = (start_node_id, end_node_id, rel_type_label)
                        edge_counts[edge_key] += 1
                        processed_relationships += 1

                        current_count = edge_counts[edge_key]
                        if current_count > 1 and len(duplicate_examples) < max_example_groups:
                            if len(duplicate_examples[edge_key]) < max_examples_per_group:
                                duplicate_examples[edge_key].append(data)
                    else:
                        skipped_lines += 1

                else:
                    skipped_lines += 1


            except json.JSONDecodeError:
                skipped_lines += 1
            except Exception as e:
                skipped_lines += 1


    num_unique_combinations = len(edge_counts)
    num_duplicates = processed_relationships - num_unique_combinations
    duplicate_percentage = (num_duplicates / processed_relationships * 100) if processed_relationships > 0 else 0

    print("\n--- Analysis Summary ---")
    print(f"Total lines read from sample: {total_lines}")
    print(f"Lines skipped (non-relationship, bad JSON, invalid key): {skipped_lines}")
    print(f"Relationship lines processed: {processed_relationships}")
    print(f"Unique (start, end, type) combinations found: {num_unique_combinations}")
    print(f"Duplicate relationship lines found (same start, end, type): {num_duplicates}")
    print(f"Percentage of duplicates among processed relationships: {duplicate_percentage:.2f}%")

    if num_duplicates > 0 and duplicate_examples:
        print(f"\n--- Example Duplicate Groups (showing up to {max_example_groups} groups) ---")
        example_groups_shown = 0
        for key, count in edge_counts.most_common():
            if count > 1 and example_groups_shown < max_example_groups:
                print(f"\nGroup Key (start, end, label): {key}") 
                print(f"Total Count in Sample: {count}")
                print(f"Showing first {min(len(duplicate_examples[key]), max_examples_per_group)} duplicates found for this group:")
                for i, edge in enumerate(duplicate_examples[key]):
                    if i >= max_examples_per_group: break
                    props_str = json.dumps(edge.get('properties', {}))
                    if len(props_str) > 150:
                        props_str = props_str[:147] + '...'
                    neo4j_id = edge.get('id', 'N/A')
                    print(f"  - Edge {i+1} (Neo4j ID: {neo4j_id}): properties={props_str}")
                example_groups_shown += 1
            elif example_groups_shown >= max_example_groups:
                break 

except FileNotFoundError:
    print(f"Error: Sample file not found at {sample_file}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
    sys.exit(1)
