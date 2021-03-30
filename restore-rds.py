import boto3
import logging
import os
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('rds')


def lambda_handler(event, context):
    # 環境変数からsnapshot_idを取得
    snapshot_id = os.environ.get('SNAPSHOT_ID')
    # 環境変数にsnapshot_idが登録されていなければinstance_idを使って取得
    if not snapshot_id:
        logger.info('START get_snapshot_id')
        snapshot_id = get_snapshot_id()
        logger.info('END get_snapshot_id')
        # instance_idから取得できなければ処理終了
        if snapshot_id is None:
            logger.info('not found snapshot')
            return

    logger.info('snapshot_id:' + snapshot_id)

    # 該当インスタンスがない場合だけrestoreを実行する
    if not is_exist_instance():
        # restore実行
        logger.info('START restore')
        restore(snapshot_id)
        logger.info('END restore')

        # セキュリティグループ&DBパラメータグループを書き換えるスクリプト実行
        logger.info('START call_modify_lambda')
        call_modify_lambda()
        logger.info('END call_modify_lambda')
    else:
        logger.info('exist instance')


def restore(snapshot_id):
    logger.info('restore snapshot id:' + snapshot_id)
    # デフォルトセキュリティグループ&DBパラメータグループで作成されるAPI仕様なので注意
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_db_snapshots
    client.restore_db_instance_from_db_snapshot(
        DBInstanceIdentifier=os.environ.get('INSTANCE_ID'),
        DBSnapshotIdentifier=snapshot_id,
        DBInstanceClass='db.t2.micro',
        Port=5432,
        AvailabilityZone='ap-northeast-1a',
        DBSubnetGroupName='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        MultiAZ=False,
        PubliclyAccessible=True,
        AutoMinorVersionUpgrade=True,
        OptionGroupName='default:postgres-9-6',
        StorageType='standard',
        CopyTagsToSnapshot=False
    )


def is_exist_instance():
    # インスタンスリストから該当インスタンスがあるかチェック
    db_instances = client.describe_db_instances()
    target_instance = \
        filter(lambda x: x['DBInstanceIdentifier'] == os.environ.get('INSTANCE_ID'), db_instances['DBInstances'])
    return len(target_instance) > 0


def get_snapshot_id():
    # 該当インスタンスのDBスナップショットをリストで取得
    snapshot_list = client.describe_db_snapshots(
        DBInstanceIdentifier=os.environ.get('INSTANCE_ID'),
        # 全Snapshot取得
        SnapshotType='manual',
    )

    logger.info(snapshot_list)
    # snapshotが帰ってくれば返す
    snapshot_id = \
        snapshot_list['DBSnapshots'][0]['DBSnapshotIdentifier'] if len(snapshot_list['DBSnapshots']) > 0 else None

    return snapshot_id


def call_modify_lambda():
    params = {'instance_id': os.environ.get('INSTANCE_ID'), 'retry_count': 0, 'modified_flag': False}
    client_lambda = boto3.client('lambda')
    client_lambda.invoke(
        FunctionName='modify-rds',
        InvocationType='Event',
        Payload=json.dumps(params)
    )
