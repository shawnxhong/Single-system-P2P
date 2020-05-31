# Single-system-P2P
A P2P file sharing network on single system that mimic a distributed p2p network using different sockets as independent peers

The main file is p2p.py, written in Python3. Only the input() method is written in old python 2.7 style as raw_input().  


How to run the code :

Type in :
	bash init_python.sh
to run the code and initialise a p2p network

To join the network, type in 
	xterm -hold – title “Peer 15” -e “python3 p2p.py join 15 4 30” &
in the uxterm to tell peer 4 that peer 15 is joining

To leave the network gracefully,, type in
	quit
or 	Quit
in the xterm of that peer

To leave the network abruptly, just press Ctrl + c in the xterm of that peer

To insert data, type in:
	Store **** (4-digit file name)
or 	store ****
in any one of these peers

To request for a file, type in:
	Request **** (4-digit file name)
or 	request ****
in any one of these peers
