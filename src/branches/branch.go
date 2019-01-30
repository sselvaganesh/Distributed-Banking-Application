package main

import (
	"../IP_Address"
	"../protobuf_bank"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

//-------------------
const maxBytes = 8192

var myTotalConnection = 0
var myNetwork []uint32

var myBranch = new(bank.InitBranch_Branch)
var allBranches = []*bank.InitBranch_Branch{}

var timeInterval = 0

//
type branchConfig struct {
	ID         int
	Name       string
	IP         string
	Port       uint32
	TCPAddress *net.TCPAddr
	Channel    *net.TCPConn
}

var myBranchConnections = []*branchConfig{}

//------------  For Branch Balance ---------------

var initBranchSuccessful = false
var branchInitBalance uint32 = 0

type branchCS struct {
	BranchCurBalance uint32
	mtx              sync.Mutex
}

var balance branchCS

//Add Balance
func (cs *branchCS) AddBalance(fund uint32) {
	cs.mtx.Lock()
	cs.BranchCurBalance += fund
	cs.mtx.Unlock()
}

//Subtract Balance
func (cs *branchCS) SubBalance(fund uint32) {
	cs.mtx.Lock()
	cs.BranchCurBalance -= fund
	cs.mtx.Unlock()
}

//Read Current Balance
func (cs *branchCS) ReadBalance() uint32 {
	cs.mtx.Lock()
	defer cs.mtx.Unlock()
	return cs.BranchCurBalance
}

//------------- Random Number Generation -------------

var s1 = rand.NewSource(time.Now().UnixNano())
var r1 = rand.New(s1)
var r2 = rand.New(s1)

//------------- Snapshot -----------------------------

var takingSnapShot = false

type newSnapShot struct {
	SnapShotID     uint32
	Balance        uint32
	ChannelState   map[string]bool
	ChannelVal     map[string]uint32
	SnapShotStatus bool
}

var currentSnapShot = new(newSnapShot)
var snapShotCollection = make(map[uint32]newSnapShot)

//Need to delete the variables
var transferID = 0
var receiveID = 0

//
var serverIP net.IP

//----------------------------------------------------------------------------------------//

func main() {

	//Get Public IP of the Server
	serverIP = IP_Address.GetPublicIP()
	fmt.Println("Branch IP:", serverIP)

	//Receive Input Parameters
	myBranch.Name = os.Args[1]
	myBranch.Ip = serverIP.String()
	temp, _ := strconv.Atoi(os.Args[2])
	myBranch.Port = uint32(temp)
	timeInterval, _ = strconv.Atoi(os.Args[3])

	//Print Input Parameters
	fmt.Println(myBranch.Name, ":", myBranch.Port, ":", timeInterval)

	//Construct TCP connection Address
	branchAddress := serverIP.String() + ":" + fmt.Sprint(myBranch.Port)

	//Allocate Memory
	currentSnapShot.ChannelState = make(map[string]bool)
	currentSnapShot.ChannelVal = make(map[string]uint32)

	//For Input Request Processing
	go BranchReceiverRequest(branchAddress)

	for !initBranchSuccessful {
		continue
	}

	//For Send Request
	go BranchSenderRequest()

	for {
		continue
	}

	//End Branch
	fmt.Println(myBranch.Name, " Closing...!!!")

}

//----------------------------------------------------------------------------------------//

func BranchReceiverRequest(branchAddress string) {

	myListener, err := net.ResolveTCPAddr("tcp", branchAddress)

	if err != nil {
		log.Fatal(err)
	}

	//Branch Listening
	branchConn, err := net.ListenTCP("tcp", myListener)
	if err != nil {
		log.Fatal(err)
	}

	//Accepts Incoming Requests
	for {

		//Receive Branch Input
		//fmt.Println("RECEIVER: Waiting for the input.")
		branchSocket, err := branchConn.AcceptTCP()

		if err != nil {
			fmt.Println("RECEIVER: Error while accepting request.!", err)
		}

		//Process Incoming Requests
		BranchReceiverHandler(branchSocket)

	}

}

//----------------------------------------------------------------------------------------//

//func BranchReceiverHandler(branchSocket net.Conn) {
func BranchReceiverHandler(branchSocket *net.TCPConn) {

	//fmt.Println("-------------------------------------------")

	//Read Branch Input as Received
	inpReqBuff := make([]byte, maxBytes)


	_, err := branchSocket.Read(inpReqBuff)

	//If No request to process, return
	if err == io.EOF {
		fmt.Println("RECEIVER: Request received as EOF..!!!!!")
		return
	}

	if err != nil {
		fmt.Println("RECEIVER: Error while reading request.!", err)
		return
	}

	//Get the Input Branch Message
	branchMsg := new(bank.BranchMessage)
	branchMsg.Reset()
	proto.Unmarshal(inpReqBuff, branchMsg)

	// 1. Branch Init Message
	if branchInitMsg := branchMsg.GetInitBranch(); branchInitMsg != nil {

		//Set the Branches and Initial Balance
		SetInitBranch(*branchInitMsg)

		//Set Init Branch Successful to Start Sending Fund Between branches
		initBranchSuccessful = true

	}

	//If Init Branch Message is not Received, Do not process any other messages
	if !initBranchSuccessful {
		fmt.Println("RECEIVER: Transfer/Marker/InitSnapShot/RetrieveSnapshot Received Before InitBranch Request...!!!!")
		return
	}

	// 2. Branch Transfer Request Message
	if branchTransferMsg := branchMsg.GetTransfer(); branchTransferMsg != nil {

		//Deleted this
		receiveID++

		//Set Branch Current Balance
		balance.AddBalance(branchTransferMsg.Money)

		fmt.Println("RECEIVER: ID:", receiveID, "Fund Received.", branchTransferMsg.SrcBranch, "->", branchTransferMsg.DstBranch, "Money:", branchTransferMsg.Money, "; New Balance:", balance.ReadBalance())

		//If the Snapshot is being Taken, Add the Messages to the Channel States
		if TakingSnapShot() {

			//Record SnapShot From Other Channels
			currentSnapShot.ChannelVal[branchTransferMsg.SrcBranch] = branchTransferMsg.Money

		}

	}

	// 3. Branch Init Snap Shot Message
	if branchInitSnapShotMsg := branchMsg.GetInitSnapshot(); branchInitSnapShotMsg != nil {

		fmt.Println("Init Snapshot Msg Received.!!!!!!!!!!!!!!!!!!!!")

		//Halt Further Transfer Fund Request
		HoldFundTransfer()

		//Set Snapshot ID and Channel States
		ResetForNewSnapshot(branchInitSnapShotMsg.SnapshotId)

		//Send Marker to All Branches
		SendMarkerToAllBranches(branchInitSnapShotMsg.SnapshotId)

	}


	// 4. Branch Marker Message
	if branchMarkerMsg := branchMsg.GetMarker(); branchMarkerMsg != nil {


		//Set the Channel State as RECEIVED (True) for the Source Branch
		currentSnapShot.ChannelState[branchMarkerMsg.SrcBranch] = true

		//fmt.Println("Marker Message Received From", branchMarkerMsg.SrcBranch)


		//If the SnapShot is Already Being Taken Mark the Channel State as RECEIVED
		if TakingSnapShot() {

			//If All the Marker Messages are RECEIVED, Release the Fund Transfer Request
			if AllMarkersReceived() {

				//Set SnapShot Status as Available = TRUE
				currentSnapShot.SnapShotStatus = true

				//Add the Snapshot to the Snapshot Collection
				snapShotCollection[branchMarkerMsg.SnapshotId] = *currentSnapShot

				//fmt.Println("All Markers are received.")
				//fmt.Println("Current Snapshot: ", *currentSnapShot)
				//fmt.Println("Snapshot Collection: ", snapShotCollection[branchMarkerMsg.SnapshotId])

				//Release Fund Transfer Request
				ReleaseFundTransfer()
			}

		} else {

			//Halt Further Transfer Fund Request
			HoldFundTransfer()

			//Set Snapshot ID and Channel States
			ResetForNewSnapshot(branchMarkerMsg.SnapshotId)

			//Mark the First Marker Message as Received
			currentSnapShot.ChannelState[branchMarkerMsg.SrcBranch] = true

			//Send Marker Messages to All Branches
			SendMarkerToAllBranches(branchMarkerMsg.SnapshotId)

		}

	}

	// 5. Branch Retrieve Snap Shot Message
	if branchRetrieveSnapShotMsg := branchMsg.GetRetrieveSnapshot(); branchRetrieveSnapShotMsg != nil {

		if !snapShotCollection[branchRetrieveSnapShotMsg.SnapshotId].SnapShotStatus {
			fmt.Println("RECEIVER: Retrieve Snapshot Received from Controller. Snapshot is still being taken.!")
			return
		}

		//Get the Snapshot ID
		thiSnapShotID := branchRetrieveSnapShotMsg.SnapshotId


		//Return SnapShot Message
		thisBranchSnapShot := new(bank.ReturnSnapshot)
		thisBranchSnapShot.LocalSnapshot = new(bank.ReturnSnapshot_LocalSnapshot)

		thisBranchSnapShot.LocalSnapshot.SnapshotId = snapShotCollection[thiSnapShotID].SnapShotID
		thisBranchSnapShot.LocalSnapshot.Balance = snapShotCollection[thiSnapShotID].Balance

		for _, thisBranch := range myBranchConnections {
			thisBranchSnapShot.LocalSnapshot.ChannelState = append(thisBranchSnapShot.LocalSnapshot.ChannelState, snapShotCollection[thiSnapShotID].ChannelVal[thisBranch.Name])
			//fmt.Println("Returning snapshot Order: ", thisBranch.Name)
		}

		//Load the SnapShot Values
		thisReturnMsg := new(bank.BranchMessage_ReturnSnapshot)
		thisReturnMsg.ReturnSnapshot = thisBranchSnapShot

		sendReturnSnapShot := new(bank.BranchMessage)
		sendReturnSnapShot.Reset()
		sendReturnSnapShot.BranchMessage = thisReturnMsg

		//Marshall ***Return SnapShot*** Message
		protoReturnSnapShotMsg, err := proto.Marshal(sendReturnSnapShot)

		if err != nil {
			fmt.Println("RECEIVER: Marshalling Error @ Send Return SnapShot.!")
		}

		_, returnSnapShotErr := branchSocket.Write(protoReturnSnapShotMsg)

		if returnSnapShotErr != nil {
			fmt.Println("RECEIVER: Error while sending Return Snapshot Message. ", myBranch.Name, "-> Controller")
		} else {
			fmt.Printf( "RECEIVER: Snapshot Sent.! ID=%v\n", thiSnapShotID )
		}

	}

	//fmt.Println("Request Finished.!")

}

//----------------------------------------------------------------------------------------//

func BranchSenderRequest() {

	for !initBranchSuccessful {
		continue
	}

	//Establish Connection
	EstablishConnection()


	//Send Fund Transfer at Regular Interval
	for  {

		if !TakingSnapShot() {

			//Send Money to any Random Branch
			TransferFund()

			//Be Inactive for the Specified Time
			DelayTransfer()

		} else {

			//fmt.Println("SENDER: Snapshot is being Taken....!!!")
			//DelayTransfer()	//Delete this..!!!!

		}

	}


	return

}

//----------------------------------------------------------------------------------------//

func SetInitBranch(branchInitMsg bank.InitBranch) {

	//Set the Current Branch Init Balance
	branchInitBalance = branchInitMsg.GetBalance()
	balance.AddBalance(branchInitMsg.GetBalance())
	allBranches = branchInitMsg.GetAllBranches()

	fmt.Println("RECEIVER: InitBranch Successful.!")

}

//----------------------------------------------------------------------------------------//

func EstablishConnection() {

	myTotalConnection = 0

	for idx, thisBranch := range allBranches {

		if !((thisBranch.Name == myBranch.Name) && (thisBranch.Port == myBranch.Port)) {

			myTotalConnection++
			myNetwork = append(myNetwork, uint32(idx))

			eachBranch := new(branchConfig)
			eachBranch.ID = myTotalConnection
			eachBranch.Name = thisBranch.Name
			eachBranch.IP = thisBranch.Ip
			eachBranch.Port = thisBranch.Port

			//var err error
			eachBranch.TCPAddress, _ = net.ResolveTCPAddr("tcp", thisBranch.Ip+":"+fmt.Sprint(thisBranch.Port))
			//eachBranch.Channel, err = net.DialTCP("tcp", nil, eachBranch.TCPAddress)
			//
			//if err != nil {
			//	fmt.Println("Establishing Connection FAILED: ", myBranch.Name, " --> ", thisBranch.Name)
			//	log.Fatal(err)
			//}
			//
			//eachBranch.Channel.Write([]byte("CONNECTION ESTABLISHED"))

			//Consolidate All the Sender Connection
			myBranchConnections = append(myBranchConnections, eachBranch)

			//fmt.Println("SENDER: Connection Established:", myBranch.Name, " --> ", thisBranch.Name)

		}

	}

	fmt.Println("SENDER: Connection Established.")
}

//----------------------------------------------------------------------------------------//

func TransferFund() {

	//Hold Transfer Until Branch is Initialized
	if !initBranchSuccessful {
		return
	}

	branchTransferMsg := new(bank.Transfer)
	branchTransferMsg.Reset()

	transferAmount := GetRandomAmount()

	if balance.ReadBalance() < transferAmount {
		fmt.Println("SENDER: Fund Transfer Cannot be Done as Transfer Amount is Greater than Balance Amount..!!!")
		return
	}

	//Set the Current Balance
	balance.SubBalance(transferAmount)

	randomBranch := GetRandomBranch()
	branchTransferMsg.SrcBranch = myBranch.Name
	branchTransferMsg.DstBranch = myBranchConnections[randomBranch].Name
	branchTransferMsg.Money = transferAmount

	//For ProtoBuf
	branchTrfMsg := new(bank.BranchMessage_Transfer)
	branchTrfMsg.Transfer = branchTransferMsg

	branchMsg := new(bank.BranchMessage)
	branchMsg.BranchMessage = branchTrfMsg

	protoTransferMsg, _ := proto.Marshal(branchMsg)

	thisReq, _ := net.DialTCP("tcp", nil, myBranchConnections[randomBranch].TCPAddress)
	_, writeError := thisReq.Write(protoTransferMsg)


	//Check Error
	if writeError != nil {
		fmt.Println("SENDER: Transfer Request Failed. ", writeError)
	} else {
		transferID++
		fmt.Println("SENDER: ID:", transferID, myBranch.Name, "->", myBranchConnections[randomBranch].Name, "Money:", branchTransferMsg.Money, myBranch.Name, "; New Balance:", balance.ReadBalance())
	}

}

//----------------------------------------------------------------------------------------//

func SendMarkerToAllBranches(snapShotID uint32) {

	branchMsg := new(bank.BranchMessage)

	//Build Marker Messages
	branchMarkerMsg := new(bank.Marker)
	branchMarkerMsg.SnapshotId = snapShotID
	branchMarkerMsg.SrcBranch = myBranch.Name

	//Send Marker to All Branches
	sendMarkerMsg := new(bank.BranchMessage_Marker)
	for _, eachBranch := range myBranchConnections {

		branchMarkerMsg.DstBranch = eachBranch.Name

		sendMarkerMsg.Marker = branchMarkerMsg

		branchMsg.Reset()
		branchMsg.BranchMessage = sendMarkerMsg

		protoMarkerMsg, err := proto.Marshal(branchMsg)

		//Error while Marshalling.?
		if err != nil {
			fmt.Println("SENDER: Marshalling error @ Marker Message. ", err)
		}

		//Send Marker Message and Check Errors
		thisReq, _ := net.DialTCP("tcp", nil, eachBranch.TCPAddress)

		_, markerErr := thisReq.Write(protoMarkerMsg)

		//_, markerErr := eachBranch.Channel.Write(protoMarkerMsg)

		if markerErr != nil {
			fmt.Println("SENDER: Error while sending Marker Message. ", myBranch.Name, "->", eachBranch.Name, markerErr)
		} //else {
		//	fmt.Println("SENDER: Marker Message Sent:", myBranch.Name, "->", eachBranch.Name)
		//}

	}

	//fmt.Println(myBranch.Name, "SENDER: All Marker Messages are Sent.!")

	return
}

//------------------ Set Channel State For Snapshots -------------------------------------//

func ResetForNewSnapshot(snapShotID uint32) {

	//Initialize the Incoming Channel State to NOT RECEIVED (false)
	currentSnapShot.SnapShotID = snapShotID
	currentSnapShot.Balance = balance.ReadBalance()
	currentSnapShot.SnapShotStatus = false //Being Taken

	//Set Each Branch Channel State as FALSE and Value as ZERO
	for _, eachBranch := range myBranchConnections {
		currentSnapShot.ChannelState[eachBranch.Name] = false
		currentSnapShot.ChannelVal[eachBranch.Name] = 0
	}

}

//------------------ All Markers Received for the Current Channel.? ----------------------------//

func AllMarkersReceived() bool {

	//Check All the Channel Message State
	for _, channelState := range currentSnapShot.ChannelState {

		if !channelState {
			return false //Incoming Marker Messages are Not Received
		}

	}

	//All Marker Messages are Received for the Current Channel
	return true

}

//------------------- Hold & Release the Fund Transfer Request -----------------------------//

//Hold
func HoldFundTransfer() {

	takingSnapShot = true
	return

}

//Release
func ReleaseFundTransfer() {

	takingSnapShot = false
	return
}

//Retrieve Snapshot Status
func TakingSnapShot() bool {

	return takingSnapShot
}

//----------------------------------------------------------------------------------------//

func GetRandomBranch() uint32 {

	//return uint32(r1.Intn(len(myNetwork) - 1))
	return uint32(r1.Intn(myTotalConnection))
}

//----------------------------------------------------------------------------------------//

func DelayTransfer() {

	time.Sleep(time.Millisecond * time.Duration(timeInterval))
	//fmt.Println("Delaying....!!!")
}

//----------------------------------------------------------------------------------------//

func GetRandomAmount() uint32 {

	amount := r2.Intn((int(branchInitBalance/100)*5)-(int(branchInitBalance)/100)) + (int(branchInitBalance) / 100)
	return uint32(amount)

}

//----------------------------------------------------------------------------------------//

func PrintMyBranchConnections() {

	fmt.Println("------------")

	for _, temp := range myBranchConnections {
		fmt.Println(temp.ID, temp.Name)
	}

	fmt.Println("------------")

}

//----------------------------------------------------------------------------------------//
