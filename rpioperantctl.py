#!/usr/bin/env python3
# this script is for starting an stopping notebooks on raspberry pis from a remote server via ssh

import subprocess
import pandas as pd
import numpy as np
import argparse
import time


def ssh_magpi(server="magpi01", is_magpi=False):
    """ opens a subprocess SSHing into magpi rpis
    
    Arguments:
        is_magpi (bool): if the current computer is magpi, or another server (e.g. txori)
    """
    if is_magpi:
        # ssh into magpi rpi
        sshProcess = subprocess.Popen(
            ["ssh", "-T", server],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            bufsize=0,
        )

    else:
        # ssh into magpi server
        sshProcess = subprocess.Popen(
            ["ssh", "-T", "bird@magpi"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            bufsize=0,
        )

        # ssh into specific magpi
        sshProcess.stdin.write("ssh -T " + server + "\n")

    return sshProcess


def get_panel_subject_behavior(
    is_magpi=False, psb_loc="/home/bird/opdat/panel_subject_behavior"
):
    """ gets panel subject behavior from magpi serverpsb_loc
    """
    if is_magpi:
        command = ["cat", psb_loc]

    else:
        command = ["ssh", "bird@magpi", "cat", psb_loc]
    cat = subprocess.Popen(command, stdout=subprocess.PIPE)
    psb = [line.decode("utf-8") for line in cat.stdout]
    return psb


def parse_panel_subject_behavior(psb):
    """ parses panel subject behavior into a pandas dataframe
    """
    process_df = pd.DataFrame(columns=["panel", "enabled", "subj", "dir", "behavior"])
    for line in psb:
        if line[0] not in ["#", "\n"]:
            splitline = line.split()
            process_df.loc[len(process_df)] = [
                splitline[0],  # panel
                splitline[1],  # enabled
                "B" + splitline[2],  # subject
                splitline[3].replace("<3>", splitline[2]),  # subject
                " ".join(splitline[4:])
                .replace("<3>", splitline[2])
                .replace("<1>", "1"),  # command
                # .replace("<1>", splitline[0]),  # command
            ]
    return process_df


def find_running_commands(server, process, user="bird", is_magpi=False):
    """determines if a specific process is running on a server (magpi)
    """
    # ssh into magpi
    sshProcess = ssh_magpi(server=server, is_magpi=is_magpi)

    # search for python processes
    sshProcess.stdin.write("ps -ef | grep '" + process + "'\n")

    # close connection
    sshProcess.stdin.close()

    # get output of commands
    out = sshProcess.stdout.readlines()

    # subset output of sshprocess to what is returned by ps -ef
    commands = []
    for line in out:
        if (
            (line[: len(user)] == user)
            & (len(line.split()) > 8)
            & (process in line)
            & ("grep" not in line)
        ):
            commands.append(line)
    return commands


def find_behavior_PID(behavior, processes_formatted, running_processes):
    """ find PID of running behavior from porcesses information
    """
    return [
        i.split()[1]
        for i in np.array(running_processes)[behavior == np.array(processes_formatted)]
    ]


def pyoperantctl(process_df, is_magpi=False):
    """ the main pioperantctl based upon the panel_subject_behavior
    """
    processes_to_kill = pd.DataFrame(columns=["Magpi", "command", "PID"])
    processes_to_start = pd.DataFrame(columns=["Magpi", "command"])

    # for
    for idx, row in process_df.iterrows():

        # find all running behavioral processes on magpi
        running_processes = find_running_commands(
            "magpi" + str(row.panel).zfill(2),
            process="pyoperant/scripts/behave",
            is_magpi=is_magpi,
        )

        # format processes to the same as in panel_subject_behavior
        processes_formatted = [
            " ".join(process.split()[-6:]).split("/")[-1]
            for process in running_processes
        ]

        # if there should be a process running
        if row.enabled == "1":

            # if the process is running
            if row.behavior in processes_formatted:
                behavior_PIDs = find_behavior_PID(
                    row.behavior, processes_formatted, running_processes
                )
                print(
                    "Panel {} | Process already running: {} | PID(s): {}".format(
                        row.panel, row.behavior, behavior_PIDs
                    )
                )
            else:
                print(
                    "Panel {} | Process needs to start: {}".format(
                        row.panel, row.behavior
                    )
                )
                processes_to_start.loc[len(processes_to_start)] = [
                    row.panel,
                    row.behavior,
                ]

        # if there should not be a process running
        elif row.enabled == "0":
            # if behavior running and should not be, kill it
            if row.behavior in processes_formatted:

                # determine PID of processesthat need to be killed
                behavior_PIDs = find_behavior_PID(
                    row.behavior, processes_formatted, running_processes
                )

                # add processes to kill to dataframe
                processes_to_kill.loc[len(processes_to_kill)] = [
                    row.panel,
                    row.behavior,
                    behavior_PIDs,
                ]

                print(
                    "Panel {} | Process needs to be killed: {} | PID(s): {}".format(
                        row.panel, row.behavior, behavior_PIDs
                    )
                )
            else:
                print(
                    "Panel {} | Process is correctly not running: {}".format(
                        row.panel, row.behavior
                    )
                )

        # run through all behaviors running, if they are not the correct behavior kill them
        for pi, process in enumerate(processes_formatted):
            # if the wrong behavior process is running it needs to be killed
            if process != row.behavior:
                print(
                    "Panel {} | Process needs to be killed: {}".format(
                        row.panel, row.behavior
                    )
                )
                # add process to list of processes to kill
                processes_to_kill.loc[len(processes_to_kill)] = [
                    row.panel,
                    running_processes[pi],
                    [running_processes[pi].split()[1]],
                ]

    return processes_to_kill, processes_to_start


def kill_behaviors(processes_to_kill, is_magpi=False):
    for idx, row in processes_to_kill.iterrows():
        for pid in row.PID:
            server = "magpi" + str(row.Magpi).zfill(2)
            print('Killing "{}" at {} in {}'.format(row.command, pid, server))
            # ssh into magpi
            sshProcess = ssh_magpi(
                server="magpi" + str(row.Magpi).zfill(2), is_magpi=is_magpi
            )

            # search for python processes
            print("kill {}".format(pid))
            sshProcess.stdin.write("kill {}".format(pid))

            # close connection
            sshProcess.stdin.close()

            # get output of commands
            # out = sshProcess.stdout.readlines()


def start_behaviors(processes_to_start, is_magpi=False):
    for idx, row in processes_to_start.iterrows():
        server = "magpi" + str(row.Magpi).zfill(2)
        print('Starting "{}" in {}'.format(row.command, server))
        # ssh into magpi
        sshProcess = ssh_magpi(server=server, is_magpi=is_magpi)

        # search for python processes
        command = "nohup /home/bird/pyoperant/scripts/" + row.command + " &"
        # print(command)
        sshProcess.stdin.write(command)

        # make sure the process is running
        # time.sleep(.25)

        # close connection
        sshProcess.stdin.close()

        # make sure command is running
        rc = find_running_commands(server, process=row.command, is_magpi=is_magpi)
        if len(rc) > 1:
            print("Start failed")

        # get output of commands
        # out = sshProcess.stdout.readlines()

        # for line in out:
        #    print(line)


def str2bool(v):
    """ allows multiple types of argument values to be equal to "True" or "False"
    """
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def get_args():
    parser = argparse.ArgumentParser(
        description="View PIDs of behaviors and kill/start them"
    )

    parser.add_argument(
        "-s",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="start behaviors that are not currently running but should be",
    )

    parser.add_argument(
        "-k",
        type=str2bool,
        nargs="?",
        const=True,
        default=False,
        help="kill behaviors that should not be running",
    )

    parser.add_argument(
        "-is_magpi",
        type=str2bool,
        nargs="?",
        const=True,
        default=True,
        help="if this script is running on magpi, or another computer (e.g. Txori)",
    )

    parser.add_argument(
        "-psb_loc",
        dest="psb_loc",
        type=str,
        default="/home/bird/opdat/panel_subject_behavior",
    )

    return parser.parse_args()


def main():
    # parse arguments
    args = get_args()
    # retrieve panel subject behavior
    psb = get_panel_subject_behavior(psb_loc=args.psb_loc, is_magpi=args.is_magpi)
    # parse panel subject behavior
    process_df = parse_panel_subject_behavior(psb)
    # find running processes, compare to panel_subject_behavior
    processes_to_kill, processes_to_start = pyoperantctl(
        process_df, is_magpi=args.is_magpi
    )
    # start/kill processes
    if args.s:
        start_behaviors(processes_to_start, is_magpi=args.is_magpi)
    if args.k:
        kill_behaviors(processes_to_kill, is_magpi=args.is_magpi)
    return


if __name__ == "__main__":
    main()
