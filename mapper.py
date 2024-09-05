#!/usr/bin/env python3

import sys

# queries -> SELECT, FILTER, GROUP BY, Aggregate Functions (MAX, MIN, SUM, AVG, COUNT), ORDER BY, SORT, JOIN, UNION, INTERSECT -> also include the queries to be able to parse different number of tables

def input_sql_query(args):
    #sql_query = input("Enter the SQL query: ")
    if args == "projection":
        sql_query = input("Enter the SQL query for projection: ")
    elif args == "filter":
        sql_query = input("Enter the SQL query for filter: ")
    elif args == "group":
        sql_query = input("Enter the SQL query for group by: ")
    return sql_query

def parse_csv_line(line):
    return line.split(',')

def get_column_indices(header, columns):
    # get the indices of the columns in the header
    if len(columns) == 1 and columns[0] == '*':
        return list(range(len(header)))
         
    indices = []
    for col in columns:
        col_name = col.strip()
        if col_name in header:
            indices.append(header.index(col_name))

    return indices
    #return [header.index(col.strip()) for col in columns if col.strip() in header]

# Projection Query
def projection_mapper():
    print("Projection Mapper called")
    sql_query = "SELECT housing_median_age, population from tripdata"
    sql_query = sql_query.lower()
    from_index = sql_query.index('from')
    columns = sql_query[6:from_index-1].strip().split(',') # columns of the query
    #columns = [col.strip() for col in sql_query.split('select')[1].split('from')[0].split(',')]
    
    is_header = True # for the first line -> getting column names

    for line in sys.stdin:
        line = line.strip()
        if is_header:
            header = parse_csv_line(line) # the column names
            column_indices = get_column_indices(header, columns)
            is_header = False
            continue
        
        values = parse_csv_line(line)
        projected_values = [values[i] for i in column_indices]
        print(','.join(projected_values))

def condition_check(header,values, condition):
    # assuming a single condition for now
    condition_parts = condition.split(' ')
    #print(condition_parts) -> ['housing_median_age', '>', '30']
    operator = condition_parts[1]
    value = condition_parts[2]
    column_name = condition_parts[0]
    column_index = header.index(column_name)

    # convert all the values in the values list into float
    try:
        values = [float(val) for val in values]
        # If conversion fails, skip this row or handle as needed
    except ValueError:
        return False
    
    i = column_index
    if operator == '>':
        if values[i] > float(value):
            return True
    elif operator == '<':
        if values[i] < float(value):
            return True
    elif operator == '=':
        if values[i] == float(value):
            return True
    elif operator == '>=':
        if values[i] >= float(value):
            return True
    elif operator == '<=':
        if values[i] <= float(value):
            return True
    elif operator == '<>' or operator == '!=':
        if values[i] != float(value):
            return True
    else:
        return False
    
    return False

# Filter Query for multiple conditions
def filter_mapper():
    print("Filter Mapper called")
    sql_query = "SELECT longitude, latitude FROM tripdata WHERE housing_median_age > 30 and median_income < 1"
    sql_query = sql_query.lower()
    from_index = sql_query.index('from')
    where_index = sql_query.index('where')
    
    # Extract columns and conditions
    columns = sql_query[7:from_index].strip().split(',')
    conditions = sql_query[where_index + 6:].strip()
    
    is_header = True

    for line in sys.stdin:
        line = line.strip()
        if is_header:
            header = parse_csv_line(line)
            column_indices = get_column_indices(header, columns)
            is_header = False
            continue
        
        values = parse_csv_line(line)
        
        # Check all conditions for the current row
        if evaluate_conditions(header, values, conditions):
            projected_values = [values[i] for i in column_indices]
            print(','.join(projected_values))

def evaluate_conditions(header, values, conditions):
    # Split conditions by AND/OR
    condition_list = split_conditions(conditions) # list of different conditions
    operators = extract_operators(conditions) # list of operators
    
    results = []
    for condition in condition_list:
        results.append(condition_check(header, values, condition.strip())) # results for each condition 
    
    final_result = results[0]
    for i, op in enumerate(operators):
        if op.upper() == 'AND':
            # the particular row is following all the conditions or not
            final_result = final_result and results[i+1]
        elif op.upper() == 'OR':
            final_result = final_result or results[i+1]
    
    return final_result

def split_conditions(conditions):
    tokens = conditions.split()
    # list of different conditions
    condition_list = []
    # current condition
    current_condition = []
    for token in tokens:
        # conditions seperated on the basis of special words of AND, OR
        if token.upper() in ('AND', 'OR'):
            condition_list.append(' '.join(current_condition))
            current_condition = []
        else:
            current_condition.append(token)
    condition_list.append(' '.join(current_condition))
    return condition_list

def extract_operators(conditions):
    tokens = conditions.split()
    return [token for token in tokens if token.upper() in ('AND', 'OR')]

def group_by_mapper():
    print("Group By Mapper called")
    sql_query = "SELECT housing_median_age, COUNT(*) FROM tripdata GROUP BY housing_median_age"
    sql_query = sql_query.lower()
    select_index = sql_query.index('select')
    from_index = sql_query.index('from')
    group_by_index = sql_query.index('group by')

    columns = sql_query[select_index+6:from_index-1].strip().split(',')

    for line in sys.stdin:
        line = line.strip()
        all_columns = parse_csv_line(line)
        break





# def filter_mapper():
#     print("Filter Mapper called")
#     # for single condition only
#     sql_query = "SELECT * FROM tripdata WHERE housing_median_age > 30"
#     sql_query = sql_query.lower()
#     from_index = sql_query.index('from')
#     where_index = sql_query.index('where')
#     columns = sql_query[6:from_index-1].strip().split(',') # columns of the query
    
#     #print("Columns: ", columns)
#     condition = sql_query[where_index+5:].strip() # condition of the query
#     is_header = True

#     for line in sys.stdin:
#         line = line.strip()
#         #print("Line: ", line)
#         if is_header:
#             header = parse_csv_line(line)
#             column_indices = get_column_indices(header, columns)
#             #print("Column indices: ", column_indices)
#             is_header = False
#             continue
        
#         values = parse_csv_line(line) # get all the column values for this line

#         bool_check = condition_check(header,values, condition)
#         if bool_check:
#             projected_values = [values[i] for i in column_indices]
#             print(','.join(projected_values))
            
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mapper.py <sql_query>")
        sys.exit(1)
    sql_query_type = sys.argv[1]
    if sql_query_type == "projection":
        projection_mapper()
    elif sql_query_type == "filter":
        filter_mapper()
    elif sql_query_type == "group":
        group_by_mapper()
    else:
        print("Invalid SQL query type")
        sys.exit(1)