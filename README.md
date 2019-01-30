# cs457-cs557-pa1-ssudala1

----------------------------------------------------------

Programming Language Opted: GO
Total files: 2
File names: branches.go; controller.go


----------------------------------------------------------

To compile the program:
-----------------------

Using Bash:
-----------

	1. Set the GOPATH environment variable as below
		1.1 Set the current working directory to "src" folder
		1.2 Execute the below command in bash
			export GOPATH=`pwd`

	2. Get the protobuf package
		2.1 Execute command "go get -u github.com/golang/protobuf/protoc-gen-go"

	3. We can directly run the programs with the below commands.
		go run branches/branch.go <BranchName> <PortNumber> <DeleyInMilliSeconds>
		go run controller/controller.go <Ledgerbalance> <InputBranchTextFileName>
	
	(Or)

	4. Create an executable for branch.go, controller.go program
		3.1 Set the current working directory to "src" folder
		3.2 Execute the below commands			
			go build branches/branch.go
			go build controller/controller.go  

	5. Then invoke the executables with the necessary inputs


Run the program:
----------------

3. Run the program by executables created
	Set the current working directory to "src" folder
	3.1 Execute the below command in bash for Branch, controller
		go run branches/branch.go <BranchName> <PortNumber> <DeleyInMilliSeconds>
		go run controller/controller.go <Ledgerbalance> <InputBranchTextFileName>


Using makefile:
--------------

	Go doesnot require makefile to create the executables as we can use "go run" command to run the program directly which will inturn take care of creating objects.



----------------------------------------------------------

Implementation:
---------------

Implemented Chandy-Lamport Algorithm using protobuf as mentioned in the assignment description

	Overview:
	--------
	1. Start the branches using the branch name, port number, time interval between transfer (milliseconds)
	2. Branch waits for controller to send InitBranch Message
	3. Once received, it starts trabsfer funds among all other branches randomly
	4. Once the controller sends init snapshot message, (Every 10 Seconds in this case)
		Branch stops sending transfer messages
		Records local balance for that branch
		Broadcast Marker messages to all other branches
		Wait for the Marker message from all other branches
		Until then record the transaction receives, in the channel states
	5. Once controller sends, return snapshot message, return the snapshot accordingly.

----------------------------------------------------------
