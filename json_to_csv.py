import sys
import json
import csv
import copy
from collections import OrderedDict

##
# Convert to string keeping encoding in mind...
##
def to_string(s):
    try:
        return str(s)
    except:
        #Change the encoding type if needed
        return s.encode('utf-8')


##
# This function converts an item like 
# {
#   "item_1":"value_11",
#   "item_2":"value_12",
#   "item_3":"value_13",
#   "item_4":["sub_value_14", "sub_value_15"],
#   "item_5":{
#       "sub_item_1":"sub_item_value_11",
#       "sub_item_2":["sub_item_value_12", "sub_item_value_13"]
#   }
# }
# To
# [
# {
#   "node_item_1":"value_11",
#   "node_item_2":"value_12",
#   "node_item_3":"value_13",
#   "node_item_4":"sub_value_14", 
#   "node_item_4_1":"sub_value_15",
#   "node_item_5_sub_item_1":"sub_item_value_11",
#   "node_item_5_sub_item_2":"sub_item_value_12",
# }, {
#   "node_item_1":"value_11",
#   "node_item_2":"value_12",
#   "node_item_3":"value_13",
#   "node_item_4":"sub_value_15", 
#   "node_item_5_sub_item_1":"sub_item_value_11",
#   "node_item_5_sub_item_2":"sub_item_value_13"
# },
# {
#   "node_item_1":"value_11",
#   "node_item_2":"value_12",
#   "node_item_3":"value_13",
#   "node_item_4":"sub_value_14", 
#   "node_item_4_1":"sub_value_15",
#   "node_item_5_sub_item_1":"sub_item_value_11",
#   "node_item_5_sub_item_2":"sub_item_value_12",
# }, {
#   "node_item_1":"value_11",
#   "node_item_2":"value_12",
#   "node_item_3":"value_13",
#   "node_item_4":"sub_value_15", 
#   "node_item_5_sub_item_1":"sub_item_value_11",
#   "node_item_5_sub_item_2":"sub_item_value_13"
# }
# ]


]
##
def reduce_item(header, row_so_far, processed_data, key, value, last):
    #Reduction Condition 1
    if type(value) is list:
        i=0
        for sub_item in value:
            reduce_item(header, copy.deepcopy(row_so_far), processed_data, key, sub_item, last)
            i=i+1

    #Reduction Condition 2
    elif type(value) is dict:
        od = OrderedDict(value)
        for sub_key in od:
            reduce_item(header, row_so_far, processed_data, key+'_'+to_string(sub_key), od[sub_key], last and (sub_key == list(od)[-1]))
    
    #Base Condition
    else:
        row_so_far[to_string(key)] = to_string(value)
        if last:
            processed_data.append(row_so_far)
            header += row_so_far.keys()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print ("\nUsage: python json_to_csv.py <node> <json_in_file_path> <csv_out_file_path>\n")
    else:
        #Reading arguments
        node = sys.argv[1]
        json_file_path = sys.argv[2]
        csv_file_path = sys.argv[3]

        fp = open(json_file_path, 'r')
        json_value = fp.read()
        raw_data = json.loads(json_value)
        fp.close()
        
        try:
            data_to_be_processed = raw_data[node]
        except:
            data_to_be_processed = raw_data

        processed_data = []
        header = []
        for item in data_to_be_processed:
            reduce_item(header, {}, processed_data, node, item, True)


        header = list(set(header))
        header.sort()

        with open(csv_file_path, 'w+') as f:
            writer = csv.DictWriter(f, header, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in processed_data:
                writer.writerow(row)

        print ("Just completed writing csv file with %d columns" % len(header))
