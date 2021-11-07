import argparse
import boto3
import csv
import json
import pymysql

output_s3_bucket = "test-hs-bucket-1"
output_s3_path = "res_tag_1015.csv"
output_file_path = "/tmp/tagged-resources_hs_1027.csv"
field_names = ['ResourceArn', 'TagKey', 'TagValue']

def select_resource_config():
    config = boto3.client('config')
    arn = []
    result = []
    config_arn = {}
    NextToken = ''

    while True:
        response = config.select_resource_config(
            Expression="SELECT * ",Limit=100, NextToken=NextToken
        )

        if 'NextToken' not in response: 
            break

        NextToken = response['NextToken']
        if NextToken == '': 
            break

        for r in response['Results']:
            js = json.loads(r) #json(string) -> dict 변환
            if 'arn' in js:
                config_arn['arn'] = js['arn']
                config_arn['doc'] = js
                # result.append(dict(config_arn))
                result.append(dict(config_arn))
    return result

def extract_tags():
    restag = boto3.client('resourcegroupstaggingapi')
    response = restag.get_resources()
    tagKey = response['ResourceTagMappingList']
    # tagKey_json = json.dumps(tagKey)
    db_tag_res(tagKey)



def db_tag_res(tag_data):
    
    ## tag_data = list of dictionary
 
    config_arn = list(map(lambda x: x['ResourceARN'], tag_data))
    config_res = list(map(lambda x: x, tag_data))
    # all_data = list(map(lambda x : x, tag_data))
    
    # print(config_arn)
    # print(config_res)

    conn = pymysql.connect(
        user="root",
        password="password",
        host="172.17.0.2",
        db="all_arn",
        charset='utf8'
    )
    
    cursor = conn.cursor(pymysql.cursors.DictCursor)    
    cursor.execute("DROP TABLE IF EXISTS config_arn")
    cursor.execute("CREATE TABLE config_arn(arn TEXT, res TEXT)")

    

    for i in tag_data:
        for key, value in i.items():
            # print(key, ":", value)
            placeholders = ', '.join(['%s'] * len(key))
            sql = "INSERT INTO %s VALUES ( %s )" % ('config_arn', placeholders )
            # print(sql)
    # for j in tag_data:
    #     for key in j:
    #         # print(key, ":", j[key])
            

    # tagged_list = dictionary
    for tagged_list in tag_data:
        columns = ', '.join(tagged_list.keys())
        placeholders = ', '.join(['%s'] * len(tagged_list))

        sql = 'INSERT INTO %s ( %s ) VALUES ( "%s" )' % ('config_arn', columns, placeholders)
        print(sql)
        # print(tagged_list.VALUES())

        cursor.execute(sql, tagged_list.values())
        # cursor.execute(sql, tagged_list.values())
        conn.commit()
        # print(tagged_list.keys())
        # # print(type(tagged_list.keys()))
        # cursor.execute(sql, tagged_list.values())
        # conn.commit()
    conn.close()   
    print(cursor.rowcount, "record insearted")
        # print(tagged_list)
        # print(placeholders)
        # print(tagged_list.keys())


  

def handler(event, context):
    # tag_data = select_resource_config()
    extract_tags()
    # db_tag_res()
    return "Done extracting tags!"

def main():
    handler({},{})

if __name__ == '__main__':
    main()
