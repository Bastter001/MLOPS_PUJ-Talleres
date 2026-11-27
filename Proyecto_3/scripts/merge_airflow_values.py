import sys
import copy
import yaml


def merge_named_list(old, new, key):
    merged = {}

    for item in (old or []) + (new or []):
        merged[item[key]] = copy.deepcopy(item)

    return list(merged.values())


def merge(base, override):

    result = copy.deepcopy(base)

    for key, value in override.items():

        if key == "env":
            result[key] = merge_named_list(
                result.get(key),
                value,
                "name"
            )

        elif key == "secret":
            result[key] = merge_named_list(
                result.get(key),
                value,
                "envName"
            )

        elif (
            isinstance(result.get(key), dict)
            and isinstance(value, dict)
        ):
            result[key] = merge(
                result[key],
                value
            )

        else:
            result[key] = copy.deepcopy(value)

    return result


values = {}

for filename in sys.argv[1:-1]:

    with open(filename) as f:

        values = merge(
            values,
            yaml.safe_load(f) or {}
        )

with open(sys.argv[-1], "w") as f:

    yaml.safe_dump(
        values,
        f,
        sort_keys=False
    )

print("MERGE_VALUES: OK")

print("\nENVIRONMENT VARIABLES:")

for item in values.get("env", []):
    print("-", item["name"])

print("\nSECRET REFERENCES:")

for item in values.get("secret", []):
    print(
        "-",
        item["envName"],
        item["secretName"],
        item["secretKey"]
    )
