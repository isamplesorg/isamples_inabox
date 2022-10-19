import json
import re

all_numeric_pattern = re.compile(r"\d+")

with open("test_model_values.json", "r") as schema_json_file:
    test_model_values_dict = json.load(schema_json_file)
    dict_copy = test_model_values_dict.copy()
    for key in dict_copy.keys():
        if all_numeric_pattern.match(key) is None:
            val = test_model_values_dict.pop(key)
            new_key = f"IGSN:{key}"
            test_model_values_dict[new_key] = val
        else:
            print(f"Skipping key {key}")
    with open("replaced_keys.json", "w") as updated_file:
        str = json.dumps(test_model_values_dict, indent=2)
        updated_file.write(str)

print("Done")
