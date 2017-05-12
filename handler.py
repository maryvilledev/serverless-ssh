import os
import json
import time

import boto3
import paramiko
from jinja2 import Template

s3 = boto3.client('s3')
dynamo = boto3.client('dynamodb')
sfn = boto3.client('stepfunctions')

script_template = """#!/bin/sh
exec > /tmp/{{ uuid }}.log
exec 2>&1
{{ commands }}
"""


def pull_key(name):
    keypath = '/tmp/'+name
    start_time = time.time()
    s3.download_file(os.getenv('CRED_BUCKET'), name, keypath)
    print("Key retrieval took %s seconds" % (time.time() - start_time))
    return keypath


def write_local_script(event_id, command):
    local_script_path = '/tmp/'+event_id+'.sh'
    script = Template(script_template)
    rendered_script = script.render(uuid=event_id, commands=command)

    file = open(local_script_path, mode='w')
    file.write(rendered_script)
    file.close()

    print("Wrote rendered script to ", local_script_path)
    return local_script_path


def get_ssh_client(user, password, host, port, key):
    thiskey = None
    if key:
        keypath = pull_key(key)
        thiskey = paramiko.RSAKey.from_private_key_file(filename=keypath)
    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("Opening SSH connection to ", host)
    client.connect(
        host,
        port=port,
        username=user,
        password=password,
        pkey=thiskey,
        look_for_keys=False,
        allow_agent=False,
        banner_timeout=20)
    return client


def poll_ssh(event, context):
    client = get_ssh_client(
        event.get('user'),
        event.get('password'),
        event.get('host'),
        event.get('port'),
        event.get('key'))

    _, stdout, _ = client.exec_command("screen -S {id} -Q select .".format(
        id=event.get('id')))

    exit_code = stdout.channel.recv_exit_status()
    result = event
    if exit_code == 1:
        result['complete'] = True

    result['poll_iteration'] += 1
    return result


def ssh_completed(event, context):
    print("Writing metadata to DynamoDB")
    dynamo.put_item(
        TableName=os.getenv('DYNAMODB_TABLE'),
        Item={
            "id": {'S': event.get('id')},
            "host": {'S': event.get('id')},
            "key": {'S': event.get('key')},
            "user": {'S': event.get('user')},
            "port": {'N': str(event.get('port', 22))},
        }
    )


def run_ssh_command(event, context):
    host = event.get('host')
    event_id = context.aws_request_id

    script_path = write_local_script(event_id, event.get('command'))

    client = get_ssh_client(
        event.get('user'),
        event.get('password'),
        host,
        event.get('port', 22),
        event.get('key'))

    # Copy the script over
    print("Copying script to ", host)
    sftp = client.open_sftp()
    sftp.put(script_path, script_path)
    sftp.chmod(script_path, 0o0755)

    print("Running script via screen")
    _, stdout, _ = client.exec_command(
        'screen -d -S {id} -m bash -c "{cmd}"'.format(
            id=event_id,
            cmd=script_path),
        environment=event.get('environment'))
    client.close()

    # if event.get('password'):
    #     s3.put_object(
    #         Body=bytes(event.get('password')),
    #         ContentType='text/plain',
    #         Key=host+'.password')
    #     print("Stored password in S3")

    sfn.start_execution(
        stateMachineArn=os.getenv('STATE_MACHINE_ARN'),
        name='ssh-poll-'+event_id,
        input=json.dumps({
            'id': event_id,
            'host': host,
            'port': event.get('port', 22),
            'user': event.get('user'),
            'password': event.get('password'),
            'key': event.get('key'),
            'complete': False,
            'poll_iteration': 0
        })
    )
