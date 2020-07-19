def normalize_identifier(identifier):
    return identifier.replace(" ", "_")\
        .replace(".", "_")\
        .replace("/", "_")


def handle_block(pool_resource_data, pool, block):
    block_resource_data = {}
    for att in pool[block]:
        block_resource_data[att] = pool[block][att]
    pool_resource_data[f"@block:{block}"] = block_resource_data


def handle_map(pool_resource_data, pool, map):
    block_resource_data = {}
    for att in pool[map]:
        block_resource_data[att] = pool[map][att]
    pool_resource_data[f"{map}"] = block_resource_data