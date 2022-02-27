'''
This Helper scrip is used to validate the BigJson file ~200,000 records of Business data.
author:sabareeswaran shanmugam
'''

import json


def validateExtraCreditDataset(working_directory):
    unvalidated_json_filename = working_directory + '/Dataset_ExrtaCredit/yelp_academic_dataset_business.json'

    with open(unvalidated_json_filename) as file:
        array = {'business': []}
        business_list = array['business']
        for line in file:
            obj = json.loads(line)
            business_list.append(obj)

    validated_json_filename = working_directory + '/Dataset_ExrtaCredit/validFormat/ExtracreditData.json'

    # Storing the validated json Dataset for Parsing
    with open(validated_json_filename, 'w') as outfile:
        outfile.write(json.dumps(array))

#print(json.dumps(array, indent=4))
#print(array)
