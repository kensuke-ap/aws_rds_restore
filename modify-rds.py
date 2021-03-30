import boto3
import logging
import os
import json
from time import sleep

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('rds')


def lambda_handler(event, context):
    # パラメータチェックstart
    if event.has_key('instance_id') and event['instance_id']:
        instance_id = event['instance_id']
    else:
        logger.info('undefined instance id')
        return

    if event.has_key('retry_count') and isinstance(event['retry_count'], int):
        retry_count = event['retry_count']
    else:
        logger.info('undefined retry count')
        return

    # インスタンス起動後の設定変更が終わっているかのフラグ
    if event.has_key('modified_flag') and isinstance(event['modified_flag'], bool):
        modified_flag = event['modified_flag']
    else:
        logger.info('undefined modified flag')
        return

    logger.info('instance id:' + instance_id)
    logger.info('retry_count:' + str(retry_count))
    logger.info('modified_flag:' + str(modified_flag))
    # パラメータチェックend

    if retry_count > os.environ.get('RETRY_MAX_COUNT'):
        logger.info('over retry count')
        return

    response = client.describe_db_instances(
        DBInstanceIdentifier=instance_id
    )
    logger.info(response)
    instance_status = response['DBInstances'][0]['DBInstanceStatus']
    logger.info('instance status:' + instance_status)
    # 設定完了かつ起動完了していたらインスタンス再起動して処理終了
    if modified_flag and instance_status == 'available':
        logger.info("START reboot")
        reboot(instance_id)
        logger.info("END reboot")
    # 設定完了かつ起動が終わっていなかったら10秒待って再実行
    elif modified_flag and instance_status == 'modifying':
        logger.info("waiting...")
        sleep(10)
        logger.info("START call_modify_lambda")
        call_modify_lambda(instance_id, retry_count, modified_flag)
        logger.info("END call_modify_lambda")
    # 設定未完了かつ起動が終わっていたらセキュリティグループとDBパラメータグループの定義を更新
    elif not modified_flag and instance_status == 'available':
        logger.info("START modify")
        modify(instance_id)
        logger.info("END modify")
        logger.info("START call_modify_lambda")
        call_modify_lambda(instance_id, retry_count, True)
        logger.info("END call_modify_lambda")
    # 設定未完了かつ上がりかけだったらちょっと待つ
    elif (not modified_flag and (
            instance_status == 'creating' or instance_status == 'backing-up' or instance_status == 'modifying')):
        logger.info("waiting...")
        sleep(120)
        logger.info("START call_modify_lambda")
        call_modify_lambda(instance_id, retry_count, modified_flag)
        logger.info("END call_modify_lambda")


# このlambdaを再実行
def call_modify_lambda(instance_id, retry_count, modified_flag):
    params = {'instance_id': instance_id, 'retry_count': retry_count + 1, 'modified_flag': modified_flag}
    client_lambda = boto3.client("lambda")
    client_lambda.invoke(
        FunctionName="modify-rds",
        InvocationType="Event",
        Payload=json.dumps(params)
    )


# インスタンス再起動
def reboot(instance_id):
    client.reboot_db_instance(
        DBInstanceIdentifier=instance_id,
        ForceFailover=False
    )


# 設定変更
def modify(instance_id):
    client.modify_db_instance(
        DBInstanceIdentifier=instance_id,
        VpcSecurityGroupIds=[
            'sg-xxxxxxxx',
        ],
        DBParameterGroupName='xxxxxxxxxxxxxxxxxxx',
    )
