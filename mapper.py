#!/usr/bin/env python3

import sys
import csv
import os
import json 



# queries -> SELECT, FILTER, GROUP BY, Aggregate Functions (MAX, MIN, SUM, AVG, COUNT), ORDER BY, SORT, JOIN, UNION, INTERSECT -> also include the queries to be able to parse different number of tables
def get_data_path(table_name):
    return f'/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/{table_name}.csv'

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
    #print("header in column indices: ", header)
    if len(columns) == 1 and columns[0] == '*':
        return list(range(len(header)))
         
    indices = []
    for col in columns:
        col_name = col.strip()
        if col_name in header:
            indices.append(header.index(col_name))

    return indices
    #return [header.index(col.strip()) for col in columns if col.strip() in header]

def get_headers(datafile):
    # if the file already exists then..
    if os.path.exists("/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers.csv"):
        with open("/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers.csv", 'r') as f:
            headers = f.readline().strip().split(',')
            #print("existing headers")
        return headers
    
      # Open the input data file
    with open(datafile, 'r') as f:
        # Read the first line as the headers
        headers = f.readline().strip().split(',')
        #print("made headers")
    
    output_csv = "/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers.csv"

    # Save headers to the output CSV file
    with open(output_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
    
    return headers

# Projection Query
def projection_mapper():
    print("Projection Mapper called")
    #sql_query = "SELECT event_time from shoppingdata"
    sql_query = "SELECT VendorID,Trip_distance, Store_and_fwd_flag from largetripdata"
    #sql_query = "SELECT * from tripdata"
    from_index = sql_query.lower().index('from')
    columns = sql_query[6:from_index-1].strip().split(',') # columns of the query
    #columns = [col.strip() for col in sql_query.split('select')[1].split('from')[0].split(',')]
    table_name = sql_query[from_index + 5:].strip()
    datafile = get_data_path(table_name)
    headers = get_headers(datafile)
    #is_header = True # for the first line -> getting column names
    column_indices = get_column_indices(headers, columns)

    for line in sys.stdin:
        line = line.strip()
        if line == (',').join(headers):
            continue #skip the headers lines for evaluation
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

    i = column_index
    # convert all the values in the values list into float
    try:
        col_value = float(values[i])
        # If conversion fails, skip this row or handle as needed
    except ValueError:
        #print("values: ", values)
        print("ValueError: Values in string can;t be converted to float")
        return False
     
    if operator == '>':
        if col_value > float(value):
            return True
    elif operator == '<':
        if col_value < float(value):
            return True
    elif operator == '=':
        if col_value == float(value):
            return True
    elif operator == '>=':
        if col_value >= float(value):
            return True
    elif operator == '<=':
        if col_value <= float(value):
            return True
    elif operator == '<>' or operator == '!=':
        if col_value != float(value):
            return True
    else:
        return False
    
    return False


def evaluate_conditions(header, values, conditions):
    # Split conditions by AND/OR
    condition_list = split_conditions(conditions) # list of different conditions
    #print("condition_list: ", condition_list)
    operators = extract_operators(conditions) # list of operators
    #print("operators: ", operators)
    
    results = []
    for condition in condition_list:
        res = condition_check(header, values, condition.strip())
        #print("res: ", res)
        results.append(res) # results for each condition 

    final_result = results[0]
    #print("final result ", final_result)
    
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


# Filter Query for multiple conditions
def filter_mapper():
    print("Filter Mapper called")
    #sql_query = "Select housing_median_age,median_income,longitude,latitude from tripdata WHERE housing_median_age > 30"
    #sql_query = "SELECT * FROM largetripdata WHERE VendorID = 2"
    sql_query = "SELECT VendorID, passenger_count, pickup_latitude, pickup_longitude FROM largetripdata WHERE VendorID = 2 and passenger_count > 5"

    from_index = sql_query.lower().index('from')
    where_index = sql_query.lower().index('where')

    table_name = sql_query[from_index+5:where_index].strip()
    datafile = get_data_path(table_name)
    headers = get_headers(datafile)
    
    # Extract columns and conditions
    columns = sql_query[7:from_index].strip().split(',')
    conditions = sql_query[where_index + 6:].strip()
    
    column_indices = get_column_indices(headers, columns)

    for line in sys.stdin:
        line = line.strip()
        if line == ','.join(headers):
            continue

        values = parse_csv_line(line)
        
        # Check all conditions for the current row
        if evaluate_conditions(headers, values, conditions):
            projected_values = [values[i] for i in column_indices]
            projection = ','.join(projected_values)
            print(projection)

def extract_agg_functions(columns, all_columns):
    """ Extracts aggregate functions from the SELECT clause. """
    agg_functions = {}
    for column in columns:
        if '(' in column and ')' in column:
            func_name = column.split('(')[0].strip().upper()
            inner_text = column[column.index('(')+1:column.index(')')].strip()
            agg_functions[inner_text] = func_name
    return agg_functions


def group_by_mapper():
    print("Group By Mapper called")
    sql_query = "SELECT VendorID, COUNT(trip_distance) FROM largetripdata GROUP BY VendorID"
    #sql_query = "Select housing_median_age, SUM(median_income) from tripdata GROUP BY housing_median_age"
    try:
        select_index = sql_query.lower().index('select')
        from_index = sql_query.lower().index('from')
        group_by_index = sql_query.lower().index('group by')
    except ValueError as e:
        print(f"Error parsing SQL query: {e}")
        return
      
    grouping_columns = sql_query[group_by_index+9:].strip().split(',')
    columns = sql_query[select_index+7:from_index].strip().split(',')
    table_name = sql_query[from_index+5:group_by_index].strip()
    datafile = get_data_path(table_name)
    headers = get_headers(datafile)
    
    #headers = parse_csv_line(header_line)
    group_indices = get_column_indices(headers, grouping_columns)
    agg_functions = extract_agg_functions(columns, headers)
    
    with open('agg_functions.json', 'w') as f:
        json.dump(agg_functions, f)


    for line in sys.stdin:
        line = line.strip()
        values = parse_csv_line(line)
        
        if values != headers:
            try:
                values = [float(value) for value in values]
            except ValueError:
                pass

            key = tuple(values[index] for index in group_indices)
            output = list(key)
            
            # Append values in the order of aggregation functions
            for col, func in agg_functions.items():
                col_index = headers.index(col)
                output.append(values[col_index])
            
        # Format key as a zero-padded string to ensure correct sorting
            formatted_key = '\t'.join(f"{float(x):10.6f}" for x in key)
            formatted_output = '\t'.join(f"{float(x):10.6f}" for x in output[len(key):])
            print(f"{formatted_key}\t{formatted_output}")


def header_files(data_file1, data_file2):
    
    if os.path.exists("/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers1.csv") and os.path.exists("/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers2.csv"):
        with open("/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers1.csv", 'r') as f1, open("/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers2.csv", 'r') as f2:
            headers1 = f1.readline().strip().split(',')
            headers2 = f2.readline().strip().split(',')
        return headers1, headers2
    
    # Open the input data file
    with open(data_file1, 'r') as f:
        # Read the first line as the headers
        headers1 = f.readline().strip().split(',')

    with open(data_file2, 'r') as f:
        # Read the first line as the headers
        headers2 = f.readline().strip().split(',')
    
    output_csv1 = "/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers1.csv"
    output_csv2 = "/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/headers2.csv"

    # Save headers to the output CSV file
    with open(output_csv1, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers1)

    with open(output_csv2, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers2)
    
    return headers1, headers2


# Join 2 tables -> inner join for now
def join_mapper():
    print("Join Mapper called")
    sql_query = "SELECT passenger_count FROM largetripdata JOIN largetripdata2 ON largetripdata.dropoff_longitude = largetripdata2.dropoff_longitude"
    #sql_query = "SELECT * from tripdata join tripdata on tripdata.longitude = tripdata.longitude"
    select_index = sql_query.lower().index('select')
    join_index = sql_query.lower().index('join')
    on_index = sql_query.lower().index('on')
    from_index = sql_query.lower().index('from')
    
    table1 = sql_query[from_index+5:join_index].strip()
    table2 = sql_query[join_index+5:on_index].strip()
    
    columns = sql_query[select_index + 7 : from_index].strip().split(',')

    # Extract the join condition columns
    join_condition = sql_query[on_index+3:].strip()
    join_columns = join_condition.split('=')
    table1_join_column = join_columns[0].strip().split('.')[-1]
    table2_join_column = join_columns[1].strip().split('.')[-1]


    # by default the join type is inner
    join_type = "inner"
    if "left join" in sql_query.lower():
        join_type = "left"
    elif "right join" in sql_query.lower():
        join_type = "right"
    elif "outer join" in sql_query.lower():
        join_type = "outer"
    elif "natural join" in sql_query.lower():
        join_type = "natural"
    
    table1_data = {}
    table2_data = {}

    data_file1 = f"/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/{table1}.csv"
    data_file2 = f"/mnt/c/Users/hp/OneDrive/Desktop/hadoop_database/{table2}.csv"

    headers1, headers2 = header_files(data_file1, data_file2)
    
    with open(data_file1, 'r') as f:
        reader = csv.reader(f)
        #headers1 = next(reader)
        for row in reader:
            key = row[headers1.index(table1_join_column)]
            #print("key1: ", key)
            table1_data[key] = row
    
    with open(data_file2, 'r') as f:
        reader = csv.reader(f)
        #headers2 = next(reader)
        for row in reader:
            key = row[headers2.index(table2_join_column)]
            #print("key2: ", key)
            table2_data[key] = row
    
    

    
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
    elif sql_query_type == "join":
        join_mapper()
    else:
        print("Invalid SQL query type")
        sys.exit(1)
