import json


# If we have new type, then add here and write the function definition
transform_func = {
    'INPUT': 'select',
    'FILTER': 'where',
    'SORT': 'order_by',
    'TEXT_TRANSFORMATION': 'transform',
    'OUTPUT': 'limit'
}
node_mapping_with_key = {}
query = {
    'select': '',
    'from': '',
    'condition': ''
}


def select(node, parent):
    transform_data = node_mapping_with_key[node]['transformObject']

    # Type
    query['select'] = 'SELECT '

    # Fields
    for field in transform_data['fields']:
        query['select'] = query['select'] + '`' + field + '`' + ', '
    query['select'] = query['select'].rstrip(', ')

    # Table name
    query['from'] = ' FROM ' + '`' + transform_data['tableName'] + '`'

    return query


def where(node, parent):
    transform_data = node_mapping_with_key[node]['transformObject']

    # Table name
    query['from'] = ' FROM ' + '`' + parent + '` '

    # Type
    query['condition'] = 'WHERE '

    # Fields
    query['condition'] = query['condition'] + '`' + transform_data['variable_field_name'] + '` '

    # Operator
    for operation in transform_data['operations']:
        query['condition'] = query['condition'] + operation['operator'] + ' ' + operation['value']

    return query


def order_by(node, parent):
    transform_data = node_mapping_with_key[node]['transformObject']

    # Table name
    query['from'] = ' FROM ' + '`' + parent + '` '

    # Type
    query['condition'] = 'ORBER BY '
    for order in transform_data:
        query['condition'] = query['condition'] + '`' + order['target']+ '` ' + order['order'] + ', '
    query['condition'] = query['condition'].rstrip(', ')

    return query


def transform(node, parent):
    transform_data = node_mapping_with_key[node]['transformObject']

    # Table name
    query['from'] = ' FROM ' + '`' + parent + '` '

    # Condition
    query['condition'] = ''

    # Fields
    for item in transform_data:
        new_field = item['transformation'] + '(`' + item['column'] + '`) as `' + item['column'] + '`'
        query['select'] = query['select'].replace('`'+item['column']+'`', new_field)

    return query


def limit(node, parent):
    transform_data = node_mapping_with_key[node]['transformObject']

    # Fields
    query['select'] = 'SELECT *'

    # Table name
    query['from'] = ' FROM ' + '`' + parent + '` '

    # Condition
    query['condition'] = 'limit ' + str(transform_data['limit']) + ' offset ' + str(transform_data['offset'])

    return query


def build_the_query(node, parent):
    node_data = node_mapping_with_key[node]
    if 'type' in node_data:
        func_name = transform_func[node_data['type']]
        func_name = func_name + '(node, parent)'
        return eval(func_name)
    else:
        print('type index not found')
        return ''


def get_query():
    return query['select'] + query['from'] + query['condition']


def process_nodes(nodes):
    result = 'With '
    for node in nodes:
        if node in node_mapping_with_key:
            build_the_query(node, nodes[node])
            result = result + node + ' as (' + get_query() + ')' + ',\n'
            print('Node ', node, ': ', get_query())
        else:
            print('Node data doesn\'t exists.')

    # Write into file
    f = open('result.sql', 'w')
    f.write(result)
    f.close()


def node_index_mapping(nodes):
    node_mapping = {}
    for node in nodes:
        node_mapping[node['key']] = node

    return node_mapping


def get_node_sequence(edges):
    nodes = {}
    for edge in edges:
        nodes[edge['from']] = ''
        nodes[edge['to']] = ''

    parent = ''
    for node in nodes:
        nodes[node] = parent
        parent = node

    return nodes


if __name__ == "__main__":
    input_file = 'request-data.json'
    try:
        with open(input_file) as json_file:
            # Parse json to dict
            data = json.load(json_file)

            # Parse the nodes just one time
            node_mapping_with_key = node_index_mapping(data['nodes'])

            # Get node sequence
            node_sequence = get_node_sequence(data['edges'])

            # Process node
            process_nodes(node_sequence)
    except EnvironmentError:
        print(input_file, ' not found')
