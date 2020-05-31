#!/usr/bin/env python3
# Author: Hong Xiaoheng
# coding: uft-8

from socket import *
import threading
import time
import datetime as dt
import sys
import os

join_type = sys.argv[1]             # "init" or "join"

if join_type == "init":
    peerID = int(sys.argv[2])       # e.g. 2
    successor1 = int(sys.argv[3])   # e.g. 4
    successor2 = int(sys.argv[4])   # e.g. 5
    pingInterval = int(sys.argv[5]) # e.g. 20
    isJoin = False
elif join_type == "join":
    peerID = int(sys.argv[2])
    knownPeer = int(sys.argv[3])
    pingInterval = int(sys.argv[4])
    isJoin = True
    successor1 = 0
    successor2 = 0
else:
    print("invalid input type")

predecessors = []  # to store the predecessor id of a peer
filestore = []
#counters for the pinging with successor1 to check whether it's alive
pingCount_successor1 = 0      
pingRecvCount_successor1 = 0  

host = "127.0.0.1" # only do this on local host
basePort = 12000   # each peer's port will be id + baseport

t_lock=threading.Condition()

# would communicate with clients after every second
timeout=False
# flag for killing the threads
stop_threads = False

# initialize the udp/tcp socket for this peer
# two udp sockets for a peer, one for sending and one for receiving
udpSendSocket = socket(AF_INET, SOCK_DGRAM) # for sending ping from a random port

udpRecvSocket = socket(AF_INET, SOCK_DGRAM) # for receiving ping or ping response, always on
udpRecvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
udpRecvSocket.bind((host, basePort + peerID))

# An always-on tcp sockets for receiving
tcpRecvSocket = socket(AF_INET, SOCK_STREAM)
tcpRecvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
tcpRecvSocket.bind((host, basePort + peerID))
tcpRecvSocket.listen(5)

def closerPredecessor(predecessors):
    #return the closer predecessor
    if predecessors[0] < predecessors[1]:
        return predecessors[1]
    else:
        return predecessors[0]

def farerPredecessor(predecessors):
    #return the farer predecessor
    if predecessors[0] > predecessors[1]:
        return predecessors[1]
    else:
        return predecessors[0]

def tcpMessage(message, peer):
    # send some message to a peer over tcp. The socket will be closed by the receiver
    tcpSocket = socket(AF_INET, SOCK_STREAM)
    tcpSocket.connect((host, basePort + peer))
    tcpSocket.send(message.encode())

def hashName(name):
    return int(name) % 256

def udpSendHandler():
    global t_lock
    global udpSendSocket
    global successor1
    global successor2
    global pingCount_successor1
    global stop_threads

    # the pinging is always on going
    while (1):
        if stop_threads:
            break
        # get lock
        with t_lock:
            print("-------------------new ping----------------------")
            message = "pingRequest from " + str(peerID)
            # the port for udpSendSocket is random here actullay, because it's not bond with any port
            udpSendSocket.sendto(message.encode(),(host, basePort + successor1))
            udpSendSocket.sendto(message.encode(),(host, basePort + successor2))

            pingCount_successor1 += 1

            if successor1 != 0 or successor2 != 0:
                print("Ping requests sent to Peers %d and %d " % (successor1, successor2))
            #notify other thread
            t_lock.notify()
        # sleep for ping interval
        time.sleep(pingInterval)

def udpRecvHandler():
    global t_lock
    global udpRecvSocket
    global successor1
    global successor2
    global pingRecvCount_successor1
    global stop_threads

    # an always on-going thread
    # To see what is received, and react depending on the message
    while (1):
        if stop_threads:
            break

        message, Address = udpRecvSocket.recvfrom(2048) # 2048 is the size of buffer 
        # here the port of the address is not 12000 + ID, cuz the sending socket is not binded, only the receving socket is binded
        message = message.decode().strip() # "pingType from ID"
        messageSplit = message.split(" ") 
        # get lock as we might access some shared data structures
        with t_lock:
            # from now on, what to do depends on what is received
            if messageSplit[0] == "pingRequest":
            # if receiving a ping from a predecessor
                print("Ping request message received from Peer %s." % (messageSplit[2]))

                if messageSplit[2] not in predecessors: # record its predecessors
                    predecessors.append(int(messageSplit[2]))
                
                # response the predecessor
                response = "pingResponse from" + " " + str(peerID)
                udpRecvSocket.sendto(response.encode(), (host, basePort + int(messageSplit[2])))

            elif messageSplit[0] == "pingResponse":
            # if receiving a response from a successor
                if int(messageSplit[2])== successor1:
                    pingRecvCount_successor1 += 1
                    print("Ping response #%s received from Peer %s." % (pingRecvCount_successor1, messageSplit[2]))

                if int(messageSplit[2])== successor2:
                    print("Ping response received from Peer %s." % (messageSplit[2]))                

            else:
                response = "Unknown Command"

def tcpRecvHandler():
    global successor1
    global successor2
    global tcpRecvSocket
    global predecessors
    global stop_threads
    global filestore

    # tcp listening standby, an always on-going thread
    # all message received will have a command type at the begining and a "Receiver ID Sender ID" at the tail
    while (1):
        if stop_threads:
            break

        connection, Address = tcpRecvSocket.accept()
        message = connection.recv(2048).decode().strip()
        messageSplit = message.split(" ")

        #print("*Received" + message)
        with t_lock:
            if messageSplit[0] == "joinRequestFrom":
            # "joinRequestFrom ID Receiver ID Sender ID"
                if int(messageSplit[1]) > peerID:
                # the peer trying to join is greater than the current
                    # forward this message to successor1 over tcp
                    forward = "joinRequestFrom " + str(messageSplit[1]) + " Receiver " + str(successor1) + " Sender " + str(peerID)
                    tcpMessage(forward, successor1)
                    # reply who sent the message. ACK
                    response = "MessageForwarded " + str(messageSplit[1]) + " toJoin Receiver " + str(messageSplit[5]) + " Sender " + str(peerID)
                    tcpMessage(response, int(messageSplit[5]))

                else:
                # the peer trying to join is smaller than the current, peer 19
                    # 19 telling 14 to update its successor
                    notice = "YourSuccessorToChange " + str(messageSplit[1]) + " " + str(peerID) + " newJoin " + str(messageSplit[1]) + " Receiver " + str(closerPredecessor(predecessors)) + " Sender " + str(peerID)
                    tcpMessage(notice, closerPredecessor(predecessors))
                    
                    predecessors = [] # predecessor changed, clear first, in pinging it will be updated

                    # tell the first successor to clear its record of predecessor, so 19 tell 2
                    notice = "ClearPredecessor Receiver " + str(successor1) + " Sender " + str(peerID)
                    tcpMessage(notice, successor1)

            elif messageSplit[0] == "ClearPredecessor":
                predecessors = []

            elif messageSplit[0] == "MessageForwarded": # MessageForwarded 15 toJoin Receiver ID Sender ID. ACK
                # if told this, it means the current peer doesnot need to make any change
                if messageSplit[1] != peerID:
                    print("Peer %s join request forwarded to my successor" % (messageSplit[1]))
                else:
                    print("Join request sent to known peer %s" % (knownPeer))

            elif messageSplit[0] == "YourSuccessorToChange":
            #    YourSuccessorToChange 15 19 newJoin 15 Receiver 14 Sender 19-> peer 14
            # or YourSuccessorToChange 14 15 newJoin 15 Receiver ID Sender ID-> peer 9
            # or YourSuccessorToChange 19 2 newJoin 15 Receiver ID Sender ID -> peer 15
                if messageSplit[1] == messageSplit[4]: # peer 14
                    # 1st, inform peer 15 that it is accepted
                    notice1 = "YourSuccessorToChange " + str(successor1) + " " + str(successor2) + " newJoin " + str(messageSplit[4]) + " Receiver " +str(messageSplit[4]) + " Sender " + str(peerID)
                    tcpMessage(notice1, int(messageSplit[4]))

                    # 2st, update its own successors
                    print("Peer %s join request received" % (messageSplit[4]))
                    successor1 = int(messageSplit[1])
                    print("My new first successor is Peer %d" % (successor1))
                    successor2 = int(messageSplit[2])
                    print("My new second successor is Peer %d" % (successor2))

                    # 3rd, inform peer 9, the first predecessor, to update its successors
                    notice2 = "YourSuccessorToChange " + str(peerID) + " " + str(messageSplit[4]) + " newJoin " + str(messageSplit[4]) + " Receiver " +str(closerPredecessor(predecessors)) + " Sender " + str(peerID)
                    #print(notice2)
                    tcpMessage(notice2, closerPredecessor(predecessors))

                elif messageSplit[2] == messageSplit[4]: # peer 9
                    # 1st, update its own successors
                    print("Successor Change request received")
                    successor1 = int(messageSplit[1])
                    print("My new first successor is Peer %d" % (successor1))
                    successor2 = int(messageSplit[2])
                    print("My new second successor is Peer %d" % (successor2))

                else: # peer 15
                    # 1st, update its own successors
                    print("Join request has been accepted")
                    successor1 = int(messageSplit[1])
                    print("My new first successor is Peer %d" % (successor1))
                    successor2 = int(messageSplit[2])
                    print("My new second successor is Peer %d" % (successor2))

            elif messageSplit[0] == "QuitRequest":
            # QuitRequest YourNewSuccessor ID ID Receiver ID Sender ID
                print("Peer %s will depart from the network" % (messageSplit[7]))
                successor1 = int(messageSplit[2])
                print("My new first successor is Peer %d" % (successor1))
                successor2 = int(messageSplit[3])
                print("My new first successor is Peer %d" % (successor2))

            elif messageSplit[0] == "AbruptQuit":
                # peer 8
                print("Peer %s is no longer alive" % messageSplit[1])
                successor1 = int(messageSplit[3])
                print("My new first successor is Peer %d" % successor1)
                successor2 = int(messageSplit[4])
                print("My new second successor is Peer %d" % successor2)
            
            elif messageSplit[0] == "ReturnYourSuccessor1":
                # peer 19
                notice3 = "MySuccessor1 " + str(successor1) + " Recevier " + str(farerPredecessor(predecessors)) + " Sender " + str(peerID)
                tcpMessage(notice3, farerPredecessor(predecessors))

                predecessors = []
                
                tcpMessage("ClearPredecessor", successor1)
            
            elif messageSplit[0] == "MySuccessor1":
                # when peer 9 receiver the successor information from peer 19, renew its own successor2 and output
                successor2 = int(messageSplit[1])
                print("My new second successor is Peer %d" % successor2)
            
            elif messageSplit[0] == "Store":
                if int(messageSplit[3]) > peerID:
                    notice4 = "Store " + messageSplit[1] + " hashLocation " +messageSplit[3] + " Receiver "+ str(successor1) + " Sender " + str(peerID)
                    tcpMessage(notice4, successor1)
                    print("Store " + messageSplit[1] + " request forwarded to my successor")
                else:
                    print("Store " + messageSplit[1] + " request accepted")

                    if int(messageSplit[1]) not in filestore:
                        filestore.append(int(messageSplit[1]))
            
            elif messageSplit[0] == "FileRequest":
                # FileRequest **** hashLocation ID Requester ID Receiver ID Sender ID
                if int(messageSplit[3]) > peerID:
                    notice5 = "FileRequest " + messageSplit[1] + " hashLocation " +messageSplit[3] + " Requester " + messageSplit[5] + " Receiver "+ str(successor1) + " Sender " + str(peerID)
                    tcpMessage(notice5, successor1)
                    print("Request for File "+ messageSplit[1] + " has been received, but the file is not stored here")
                else:
                    print("File "+ messageSplit[1]+ "is stored here")
                    print("Sending file " + messageSplit[1] + " to Peer " + messageSplit[5])
                    notice6 = "FileReturn "+ messageSplit[1] + " Receiver " + messageSplit[5] + " Sender "+ str(peerID)
                    tcpMessage(notice6, int(messageSplit[5]))
            
            elif messageSplit[0] == "FileReturn":
                print("Peer " + messageSplit[5] + " had File " + messageSplit[1])
                print("Receiving File "+ messageSplit[1] + " from Peer " + messageSplit[5])
                filestore.append(int(messageSplit[1]))
                print("File "+messageSplit[1]+ " received")

            connection.close()

def joinHandler():
    # handle the scenario of a peer trying the join the network when it only knows one existing peer
    if join_type == "join":
        message = "joinRequestFrom " + str(peerID) + " Receiver " + str(knownPeer) + " Sender " + str(peerID)
        tcpMessage(message, knownPeer)

def quitHandler():
    global successor1
    global successor2
    global predecessors
    global stop_threads
    #and do thing in TCP receive handler according to the command
    print("Quit command received")
    # tell the first predecessor to update
    notice1 = "QuitRequest YourNewSuccessors " + str(successor1) + " " + str(successor2) + " Receiver " + str(closerPredecessor(predecessors)) + " Sender " + str(peerID)
    tcpMessage(notice1, closerPredecessor(predecessors))

    # tell the second predecessor to update
    notice2 = "QuitRequest YourNewSuccessors " + str(closerPredecessor(predecessors)) + " " + str(successor1) + " Receiver " + str(farerPredecessor(predecessors)) + " Sender " + str(peerID)
    tcpMessage(notice2, farerPredecessor(predecessors))

    # tell the successors to clear predecessor record
    tcpMessage("ClearPredecessor", successor1)
    tcpMessage("ClearPredecessor", successor2)

    stop_threads = True
    

def abruptQuitHandler():
    global pingCount_successor1
    global pingRecvCount_successor1
    global successor1
    global successor2
    global predecessors
    global stop_threads

    # a thread to check if the current peer's successor1 is alive. only peer 9 need to do all the noticing job when such thing happens
    # thus no need to check successor2
    while (1):
        if stop_threads:
            break
        # so is should be always on as well
        if pingCount_successor1 - pingRecvCount_successor1 > 2:
            print("Peer %d is longer alive" % successor1)
            #tell peer 8 to update
            notice1 = "AbruptQuit "+ str(successor1)+" YourNewSuccessors " + str(peerID) + " " + str(successor2) + " Receiver " + str(closerPredecessor(predecessors)) + " Sender " + str(peerID)
            tcpMessage(notice1, closerPredecessor(predecessors))
            #update peer 9's successor1. it will update its successor2 when here from peer 19
            successor1 = successor2
            print("My new first successor is Peer %d" % successor1)
            # ask peer 19 for its success1 information
            notice2 = "ReturnYourSuccessor1 Receiver " + str(successor1) + " Sender " + str(peerID)
            tcpMessage(notice2, successor1)

            # update
            pingCount_successor1 = 0
            pingRecvCount_successor1 = 0

def inputHandler():
    global stop_threads
    # stand by waiting for input
    while (1):
        if stop_threads:
            break

        command = raw_input()
        command.strip()
        # this is very weired. I try input() but it raise errors in python 2.7. Have to use raw_input() as in python 2.7 to make this work
        if command == "Quit" or command == "quit":
            quitHandler()
        
        else:
            commandSplit = command.split(" ")
            # Store ****
            if commandSplit[0] == "store" or commandSplit[0] == "Store":
                filename = int(commandSplit[1])
                location = hashName(filename)
                if location > peerID:
                    message = "Store " + str(filename) + " hashLocation " +str(location) + " Receiver "+ str(successor1) + " Sender " + str(peerID)
                    tcpMessage(message, successor1)
                    print("Store " + str(filename) + " request forwarded to my successor")
                else:
                    print("Store " + str(filename) + " request accepted")

            elif commandSplit[0] == "request" or commandSplit[0] == "Request":
                filename = int(commandSplit[1])
                location = hashName(filename)
                if location > peerID:
                    message2 = "FileRequest " + str(filename) + " hashLocation " +str(location) + " Requester "+ str(peerID) +" Receiver "+ str(successor1) + " Sender " + str(peerID)
                    tcpMessage(message2, successor1)
                    print("File request for " + str(filename) + " has been sent to my successor")
                else:
                    print("File " + str(filename) + " is already stored here")
            
            else:
                    print('Invalid command')
                    pass


if isJoin == True:
    # do the joining
    joinHandler()

tcp_recv_thread = threading.Thread(name="TCPRecvHandler", target=tcpRecvHandler)
tcp_recv_thread.daemon = True
tcp_recv_thread.start()

udp_recv_thread = threading.Thread(name="UDPRecvHandler", target=udpRecvHandler)
udp_recv_thread.daemon = True
udp_recv_thread.start()

udp_send_thread = threading.Thread(name="UDPSendHandler", target=udpSendHandler)
udp_send_thread.daemon = True
udp_send_thread.start()

input_thread = threading.Thread(name="InputHandler", target=inputHandler)
input_thread.daemon = True
input_thread.start()

abruptQuit_thread = threading.Thread(name="abruptQuitMonior", target=abruptQuitHandler)
abruptQuit_thread.daemon = True
abruptQuit_thread.start()

#this is the main thread
while True:
    if stop_threads:
        break

    time.sleep(0.1)
