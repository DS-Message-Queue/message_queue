#!/bin/sh

gnome-terminal --tab -- bash -ic "python3 run_http_server.py"
gnome-terminal --tab -- bash -ic "python3 run_manager.py"
gnome-terminal --tab -- bash -ic "python3 run_replica.py"
gnome-terminal --tab -- bash -ic "python3 run_broker.py 50052 8003 8004 8005"
gnome-terminal --tab -- bash -ic "python3 run_broker.py 50054 8004 8005 8003"
gnome-terminal --tab -- bash -ic "python3 run_broker.py 50055 8005 8003 8004"
gnome-terminal --tab
