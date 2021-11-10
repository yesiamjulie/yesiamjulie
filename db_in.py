import argparse
from typing import Counter
import boto3
import csv
import json
import pymysql
from pymysql import cursors

conn = pymysql.connect(

    user="root",
    password="interpark",
    host="10.222.10.189",
    db="all_arn",
    charset='utf8')


def select_resource_config():

    config = boto3.client('config')
    arn = []
    result = []
    config_arn = {}
    NextToken = ''

    # cursor = conn.cursor(pymysql.cursors.DictCursor)
    # cursor.execute("DROP TABLE IF EXISTS all_arn")
    # cursor.execute("CREATE TABLE all_arn(arn LONGTEXT, doc JSON)")

    while True:
        response = config.select_resource_config(
            Expression="SELECT * ", Limit=100, NextToken=NextToken
        )

        if 'NextToken' not in response:
            break

        NextToken = response['NextToken']
        if NextToken == '':
            break

        for r in response['Results']:
            js = json.loads(r)  # json(string) -> dict 변환
            if 'arn' in js:
                config_arn['arn'] = js['arn']
                config_arn['doc'] = js
                result.append(dict(config_arn))

    for i in result:

        sql = "insert into all_arn values('" + \
            i['arn'] + "' , '" + json.dumps(i) + "')"
        # cursor.execute(sql)
    # conn.commit()
    # conn.close()


def extract_tags():

    restag = boto3.client('resourcegroupstaggingapi')
    response = restag.get_resources()

    cursors = conn.cursor(pymysql.cursors.DictCursor)
    cursors.execute("DROP TABLE IF EXISTS tagged_arn")
    cursors.execute("CREATE TABLE tagged_arn(arn LONGTEXT, doc JSON)")

    while 'PaginationToken' in response and response['PaginationToken']:
        token = response['PaginationToken']
        response = restag.get_resources(
            ResourcesPerPage=100, PaginationToken=token)
        for res in response['ResourceTagMappingList']:
            sql = "insert into tagged_arn values('" + \
                res['ResourceARN'] + "', '" + json.dumps(res) + "')"
            print(sql)
            cursors.execute(sql)
            conn.commit()
    conn.close()


def main():
    # select_resource_config()
    extract_tags()


if __name__ == '__main__':
    main()

    
    
    ## pymysql.err.OperationalError: (3140, 'Invalid JSON text: "Invalid encoding in string." at position 145 in value for column \'tagged_arn.doc\'.')
