import logging
import os
from uuid import uuid4

import paramiko
import yaml
from pydantic import BaseModel
from rich import print
from pathlib import Path
import datetime
import argparse

logging.basicConfig(level=logging.INFO)


class Directory(BaseModel):
    parent: str
    child_targets: list[str]


class SSHBackupTarget(BaseModel):
    name: str
    hostname: str
    username: str
    password: str
    port: int = 22
    directories: list[Directory]


def connect(
    hostname: str, username: str, password: str, port: int = 22
) -> paramiko.SSHClient:
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname, port=port, username=username, password=password)
    logging.info(f"Connected to {hostname}:{port} as {username}")
    return ssh_client


def create_tarball(
    ssh_client: paramiko.SSHClient, parent: str, target: str, target_tarball_path: str
):
    logging.info(f"Creating tarball for {parent}/{target} -> {target_tarball_path}")
    _, stdout, _ = ssh_client.exec_command(
        f"tar -cJvf {target_tarball_path} -C {parent} {target}"
    )
    logging.info("Waiting for tarball to be created...")
    exit_status = stdout.channel.recv_exit_status()
    if exit_status == 0:
        logging.info("Compressed Archive was successfully created.")
    else:
        logging.critical("Compressed Archive was not created.")


def copy_tarball(
    ssh_client: paramiko.SSHClient,
    parent: str,
    target: str,
    target_tarball_path: str,
    destination: Path,
    host_name: str,
):
    target_file_name = f"{host_name}_{target}_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.tar.xz"
    full_dest_path = destination / target_file_name
    logging.info(
        f"Copying tarball from remote:{parent}/{target} -> local:{full_dest_path}"
    )
    sftp = ssh_client.get_transport().open_sftp_client()
    sftp.get(target_tarball_path, full_dest_path)


def delete_tarball(ssh_client: paramiko.SSHClient, target_tarball_path: str):
    logging.info(f"Deleting tarball {target_tarball_path}")
    _, stdout, _ = ssh_client.exec_command(f"rm {target_tarball_path}")


def load_targets() -> list[SSHBackupTarget]:
    with open("targets.yaml", "r") as f:
        return [SSHBackupTarget(**target["ssh_target"]) for target in yaml.safe_load(f)]


def main(backups_destination_directory: Path):
    try:
        _ =  os.path.exists(backups_destination_directory) or os.mkdir(
            backups_destination_directory
        )
    except OSError as e:
        logging.error(f"Could not create {backups_destination_directory}")
        raise e
    
    
    targets: list[SSHBackupTarget] = load_targets()
    for target in targets:
        ssh_client = connect(
            target.hostname, target.username, target.password, target.port
        )
        for directory in target.directories:
            for child_target in directory.child_targets:
                target_tarball_path = f"/tmp/{uuid4()}.tar.xz"
                create_tarball(
                    ssh_client, directory.parent, child_target, target_tarball_path
                )
                copy_tarball(
                    ssh_client,
                    directory.parent,
                    child_target,
                    target_tarball_path,
                    backups_destination_directory,
                    target.name,
                )
                delete_tarball(ssh_client, target_tarball_path)
        ssh_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backup remote directories over SSH")
    parser.add_argument("-d", "--destination", type=str, required=True, help="Destination directory for backups. This directory will be created if it does not exist.")
    args = parser.parse_args()
    main(Path(args.destination))