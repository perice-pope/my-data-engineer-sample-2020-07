"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
from src.some_storage_library import SomeStorageLibrary

filename_data = 'data/source/SOURCEDATA.txt'
filename_columns = 'data/source/SOURCECOLUMNS.txt'
filename_output = 'data/output.csv'


if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')
    file_data = open(filename_data, 'r')
    file_columns = open(filename_columns, 'r')
    file_output = open(filename_output, 'w')

    columns_lines = file_columns.readlines()
    file_columns.close()
    columns_list = [None] * len(columns_lines)
    #print(columns_lines)
    #print(columns_list)
    for line in columns_lines:
        words = line.split('|')
        index = int(words[0])
        name = words[1].strip()
        columns_list[index - 1] = name
        # print(columns_list)
    #print(columns_list)

    # Writing header for csv file
    for i , column_name in enumerate(columns_list):
        file_output.write(column_name)
        if i != len(columns_list) - 1:
            file_output.write(',')
    file_output.write('\n')
    
    # Writing data in csv file
    
    while True:
        line = file_data.readline()
        if not line:
            break
        data_list = line.strip().split('|')
        # Writing data for a single line
        for i, data_item in enumerate(data_list):
            file_output.write(data_item)
            # Writing comma if not last item
            if i != len(data_list) - 1:
                file_output.write(',')
        file_output.write('\n')
    file_data.close()
    file_output.close()

    storage = SomeStorageLibrary()
    storage.load_csv(filename_output)
