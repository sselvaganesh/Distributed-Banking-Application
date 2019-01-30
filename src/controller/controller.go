package main

import (
	"../protobuf_bank"
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

//------------------------------------

//Constants

const maxBytes = 1024
const snapShotInterval = 10

//Global Variables

var totalBranches uint32 = 0
var ledgerBalance = 0

//
type createBranch struct {
	name       string
	IP         string
	Port       string
	TCPAddress *net.TCPAddr
	channel    net.Conn
}

var branchAndConn = []*createBranch{}

//
var bankBranches = new(bank.InitBranch)
var loadAllBranch = []*bank.InitBranch_Branch{}

//Snapshot ID
var snapShotID uint32 = 3

//Verify Snapshot
var snapShotAmount uint32 = 0

//------------- Random Number Generation -------------

var s1 = rand.NewSource(time.Now().UnixNano())
var r1 = rand.New(s1)

//------------------------------------

func main() {

	//Receive Input Parameters
	ledgerBalance, _ = strconv.Atoi(os.Args[1])
	branchFileName := os.Args[2]

	//Input Received
	fmt.Println("Controller: Ledger Balance=", ledgerBalance, "; Config. File Name: ", branchFileName)

	//Identify All Branches and Connect Controller with All Other Branches
	ConnectAllBranches(branchFileName)

	//Init Branch Message
	SendInitBranch()

	//Take Snapshot at Regular Interval
	for {

		time.Sleep(snapShotInterval * time.Second)

		//Init Snap Shot Message
		SendInitSnapShot()

		//Retrieve Snap Shot Message
		SendRetrieveSnapShot()

	}

	//
	fmt.Println("Controller Closing..!!!")

}

//---------------------------------------------------------------

func ConnectAllBranches(branchFileName string) {

	//Get InitBranch Structure
	bankBranches.Reset()

	//
	file, err := os.Open(branchFileName)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		os.Exit(1)
	}

	//Read Config File Content
	fileContent := bufio.NewReader(file)

	fileBuff, _, err := fileContent.ReadLine()

	for err == nil {

		var err1 error

		branch := string(fileBuff)
		branchDtl := strings.Split(branch, " ")

		//Create a Branch and TCP Connection for every instance
		newBranch := new(createBranch)
		newBranch.name = branchDtl[0]
		newBranch.IP = branchDtl[1]
		newBranch.Port = branchDtl[2]
		newBranch.TCPAddress, _ = net.ResolveTCPAddr("tcp", newBranch.IP+":"+newBranch.Port)

		//newBranch.channel, err1 = net.Dial("tcp", branchDtl[1]+":"+branchDtl[2])
		//newBranch.channel, err1 = net.DialTCP("tcp", nil, newBranch.TCPAddress)
		//

		newBranch.channel, err1 = net.DialTCP("tcp", nil, newBranch.TCPAddress)

		if err1 != nil {
			fmt.Println("TCP Connection between Controller and", branchDtl[0], " FAILED.")
			log.Fatal(err1)
		}

		//newBranch.Writer = bufio.NewWriter(newBranch.channel)
		//newBranch.Reader = bufio.NewReader(newBranch.channel)

		//Load the Branch Details in the InitBranch Structure
		newInitBranch := new(bank.InitBranch_Branch)
		newInitBranch.Reset()
		newInitBranch.Name = branchDtl[0]
		newInitBranch.Ip = branchDtl[1]
		temp, _ := strconv.Atoi(branchDtl[2])
		newInitBranch.Port = uint32(temp)

		loadAllBranch = append(loadAllBranch, newInitBranch)

		//Consolidate Branches
		branchAndConn = append(branchAndConn, newBranch)

		totalBranches++

		fileBuff, _, err = fileContent.ReadLine()

	}

	//fmt.Println("TotalBranches: ", totalBranches)

}

//---------------------------------------------------------------

func SendInitBranch() {

	branchMsg := new(bank.BranchMessage)

	//Complete String message for InitBranch
	bankBranches.Balance = uint32(ledgerBalance) / totalBranches
	bankBranches.AllBranches = loadAllBranch

	initBranchMsg := new(bank.BranchMessage_InitBranch)
	initBranchMsg.InitBranch = bankBranches
	branchMsg.BranchMessage = initBranchMsg

	protoInitBranchMsg, err := proto.Marshal(branchMsg)

	if err != nil {
		log.Fatal("Marshalling Error @ InitBranch: ", err)
	}

	//Send InitBranch Message to All the Branches
	for _, thisBranch := range branchAndConn {
		thisBranch.channel.Write(protoInitBranchMsg)

	}

}

//---------------------------------------------------------------

func SendInitSnapShot() {

	branchMsg := new(bank.BranchMessage)

	initSnapShot := new(bank.InitSnapshot)
	initSnapShot.Reset()
	initSnapShot.SnapshotId = snapShotID

	initSnapShotMsg := new(bank.BranchMessage_InitSnapshot)
	initSnapShotMsg.InitSnapshot = initSnapShot

	branchMsg.BranchMessage = initSnapShotMsg

	protoInitSnapShotMsg, err := proto.Marshal(branchMsg)

	if err != nil {
		log.Fatal("Marshalling Error @ InitSnapShot: ", err)
	}

	randomBranch := GetRandomBranch()
	//fmt.Println("Init Snap Shot: ->", branchAndConn[randomBranch].name)

	thisConn, _ := net.DialTCP("tcp", nil, branchAndConn[randomBranch].TCPAddress)
	thisConn.Write(protoInitSnapShotMsg)

	//branchAndConn[0].channel.Write(protoInitSnapShotMsg)
	//branchAndConn[randomBranch].channel.Write(protoInitSnapShotMsg)

}

//---------------------------------------------------------------

func SendRetrieveSnapShot() {

	time.Sleep(time.Second * 3)

	branchMsg := new(bank.BranchMessage)

	retSnapShot := new(bank.RetrieveSnapshot)
	retSnapShot.Reset()
	retSnapShot.SnapshotId = snapShotID

	retSnapShotMsg := new(bank.BranchMessage_RetrieveSnapshot)
	retSnapShotMsg.RetrieveSnapshot = retSnapShot

	branchMsg.BranchMessage = retSnapShotMsg

	protoRetSnapShotMsg, err := proto.Marshal(branchMsg)

	if err != nil {
		log.Fatal("Marshalling Error @ RetrieveSnapShot: ", err)
	}

	fmt.Println("------------------------------------------------------------------------------------------------")
	snapShotAmount = 0

	fmt.Println("Snapshot_ID:", snapShotID)

	//Send Retrieve Snap Shot Message to All the Branches
	for _, thisBranch := range branchAndConn {

		//thisBranch.channel.Write(protoRetSnapShotMsg) //Retrieve Snapshot
		thisConn, _ := net.DialTCP("tcp", nil, thisBranch.TCPAddress)
		thisConn.Write(protoRetSnapShotMsg) //Retrieve Snapshot

		ReadReturnedSnapShot(thisBranch, thisConn) //Return Snapshot
	}

	//Verify Snapshot Amount
	//fmt.Println("Snapshot Amount:", snapShotAmount)

	//For the Next Snapshot
	snapShotID++
}

//---------------------------------------------------------------

func ReadReturnedSnapShot(thisBranch *createBranch, oneOfTheBranch *net.TCPConn) {

	//Read Branch Input as Received
	inpReqBuff := make([]byte, maxBytes)
	_, err := oneOfTheBranch.Read(inpReqBuff)

	//If No request to process, return
	if err == io.EOF {
		fmt.Println("Reading Snapshot: Request received as EOF..!!!!!")
		return
	}

	if err != nil {
		fmt.Println("ReadSnapShot Error", err)
		return
	}

	branchMsg := new(bank.BranchMessage)
	retSnapShotMsg := new(bank.ReturnSnapshot)

	proto.Unmarshal(inpReqBuff, branchMsg)

	if retSnapShotMsg = branchMsg.GetReturnSnapshot(); retSnapShotMsg != nil {

		localSnapShot := new(bank.ReturnSnapshot_LocalSnapshot)
		localSnapShot = retSnapShotMsg.LocalSnapshot

		fmt.Print(thisBranch.name, ": ", localSnapShot.GetBalance(), ", ")

		//For Verification
		snapShotAmount += localSnapShot.GetBalance()

		channelStates := localSnapShot.GetChannelState()

		actIdx := 0
		for idx, channelVal := range channelStates {

			var srcBranch string
			if thisBranch.name == branchAndConn[idx].name {
				actIdx++
				//srcBranch = branchAndConn[actIdx].name
			} //else {
			//srcBranch = branchAndConn[idx].name
			//}

			srcBranch = branchAndConn[actIdx].name
			actIdx++

			fmt.Print(srcBranch, "->", thisBranch.name, ": ", channelVal, ", ")

			//For Verification
			snapShotAmount += channelVal

		}

		fmt.Println()

	}

	return

}

//---------------------------------------------------------------

func GetRandomBranch() uint32 {

	return uint32(r1.Intn(int(totalBranches - 1)))

}

//---------------------------------------------------------------
