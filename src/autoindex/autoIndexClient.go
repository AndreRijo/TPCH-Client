package autoindex

import (
	"bufio"
	"fmt"
	"os"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"potionDB/src/tools"
	"strconv"
	"strings"
	"tpch_client/src/client"
)

var (
	tpchInterface = client.AutoIndexTPCH{}
	db            = InitializeClientDB()
	reader        = bufio.NewReader(os.Stdin)
	ci            = antidote.CodingInfo{}.Initialize()
)

func StartAutoIndexClient() {
	tpchInterface.PrepareCommunications()
	for {
		input, _ := reader.ReadString('\n')
		input = input[:len(input)-1] //removing \n
		inputParts := strings.Split(input, " ")
		inputParts[0] = strings.ToUpper(inputParts[0])
		switch inputParts[0] {
		case GET:
			doGet(inputParts)
		case UPDATE:
			doUpdate(inputParts)
		case LINK:
			doLink(inputParts, input)
		case CREATE:
			//Can be generic or just normal trigger
			if strings.ToUpper(inputParts[1]) == GENERIC {
				doGenericTrigger(inputParts, input)
			} else {
				doTrigger(inputParts, input)
			}
		case DELETE:
			//TODO
			doDelete(inputParts)
		case GET_TRIGGERS:
			doGetTriggers()
		}

	}
}

//GET key bucket type PART_READ?
func doGet(args []string) {
	key, bucket, crdtType := args[1], args[2], args[3]
	fmt.Printf("[READ]Key %s, Bucket %s, CRDTType %s\n", key, bucket, crdtType)
	keyParams := db.getKeyParams(key, bucket, crdtType)
	db.AddObject(keyParams)
	//TODO: Finish
	if len(args) > 4 {
		fmt.Println("[WARNING]Partial reading not yet supported. Doing full read instead")
	}
	param := []antidote.ReadObjectParams{antidote.ReadObjectParams{KeyParams: keyParams}}
	reply := tpchInterface.DoGet(param)
	fmt.Println("[READ]Reply: " + getReadString(keyParams.CrdtType, reply))
}

//UPDATE key bucket type UPDATE_ARGS
func doUpdate(args []string) {
	key, bucket, crdtType := args[1], args[2], args[3]
	fmt.Printf("[UPDATE]Key %s, Bucket %s, CRDTType %s\n", key, bucket, crdtType)
	keyParams := db.getKeyParams(key, bucket, crdtType)
	db.AddObject(keyParams)
	updArgs := processUpdArgs(keyParams.CrdtType, args[4:])
	objUpd := antidote.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &updArgs}
	fmt.Println("[UPDATE]Preparing links...")
	allUpds := db.prepareLinks(objUpd)
	fmt.Print("[UPDATE]Updates gerated: ")
	for _, upd := range allUpds {
		fmt.Printf("%v: %v; ", upd.KeyParams, *upd.UpdateArgs)
	}
	fmt.Println()
	tpchInterface.DoUpdate(allUpds)
	fmt.Println("[UPDATE]Update applied.")
}

/*
	Args:
	(key, bucket, type)
	(operation name, arguments)
	(key, bucket, type)
	(operation name, arguments)
	For now, assuming all arguments on the left side are variables.
*/
func doLink(args []string, origArgs string) {
	key, bucket, crdtType := args[1], args[2], args[3]
	fmt.Printf("[LINK]Key %s, Bucket %s, CRDTType %s\n", key, bucket, crdtType)
	keyParamsObj := db.getKeyParams(key, bucket, crdtType)
	db.AddObject(keyParamsObj)

	fmt.Println("[LINK]Processing first half arguments")
	objOpName, objArgs, endPos := getOperation(args, origArgs)
	viewArgs := strings.Split(origArgs[endPos:], " ") //Note: There's probably a more efficient solution for this
	vKey, vBucket, vCrdtType := viewArgs[1], viewArgs[2], viewArgs[3]
	keyParamsView := db.getKeyParams(vKey, vBucket, vCrdtType)
	db.AddObject(keyParamsView)
	fmt.Printf("[LINK]View Key %s, Bucket %s, CRDTType %s\n", vKey, vBucket, vCrdtType)
	fmt.Println("[LINK]Processing second half arguments")
	viewOpName, viewArgs, _ := getOperation(viewArgs, origArgs[endPos:])

	firstLinkPart := processLinkOp(keyParamsObj, objOpName, objArgs)
	secondLinkPart := processLinkOp(keyParamsView, viewOpName, viewArgs)

	autoUpd := db.AddLink(firstLinkPart, secondLinkPart)
	tpchInterface.SendTrigger(autoUpd, false, ci)
}

func doGenericTrigger(args []string, origArgs string) {
	triggerName := args[3]
	ignore(triggerName)
	if len(args) == 4 {
		args, origArgs = readTriggerArgs()
	} else {
		args = args[5:]
	}

	key, bucket, crdtType := args[0], args[1], args[2]
	fmt.Printf("[GENERIC_TRIGGER]Key %s, Bucket %s, CRDTType %s\n", key, bucket, crdtType)
	matchKeyParamsObj := db.GetMatchableKeyParams(key, bucket, db.stringTypeToCrdtType(crdtType))
	keyParamsObj := db.getKeyParams(key, bucket, crdtType)

	fmt.Println("[GENERIC_TRIGGER]Processing first half arguments")
	objOpName, objArgs, endPos := getOperation(args, origArgs) //Shifting left to match opName location on LINK interface
	viewArgs := strings.Split(origArgs[endPos:], " ")          //Note: There's probably a more efficient solution for this
	//Need to skip "AS" and "UPDATE"
	vKey, vBucket, vCrdtType := viewArgs[3], viewArgs[4], viewArgs[5]
	keyParamsView := db.getKeyParams(vKey, vBucket, vCrdtType)
	db.AddObject(keyParamsView)
	fmt.Printf("[GENERIC_TRIGGER]View Key %s, Bucket %s, CRDTType %s\n", vKey, vBucket, vCrdtType)
	fmt.Println("[GENERIC_TRIGGER]Processing second half arguments")
	viewOpName, viewArgs, _ := getOperation(viewArgs[3:], origArgs[endPos:]) //Shifting left to match opName locaiton on LINK interface

	firstLinkPart := processLinkOp(keyParamsObj, objOpName, objArgs)
	secondLinkPart := processLinkOp(keyParamsView, viewOpName, viewArgs)

	autoUpd := db.AddGenericLink(matchKeyParamsObj, firstLinkPart, secondLinkPart)
	tpchInterface.SendTrigger(autoUpd, true, ci)

}

/*
	User interface:
	CREATE TRIGGER name
	ON keyParams
	WITH op
	DOW
	UPDATE keyParams
	WITH op
	At the moment parsing supports either one line or multi-line EXACTLY split as with the scheme above

*/
func doTrigger(args []string, origArgs string) {
	//TODO: Make a couple of methods that handle the similar parts of LINK, TRIGGER and GENERIC TRIGGER.
	triggerName := args[2]
	ignore(triggerName)
	if len(args) == 3 {
		args, origArgs = readTriggerArgs()
	} else {
		args = args[4:]
	}

	key, bucket, crdtType := args[0], args[1], args[2]
	fmt.Printf("[TRIGGER]Key %s, Bucket %s, CRDTType %s\n", key, bucket, crdtType)
	keyParamsObj := db.getKeyParams(key, bucket, crdtType)
	db.AddObject(keyParamsObj)

	fmt.Println("[TRIGGER]Processing first half arguments")
	objOpName, objArgs, endPos := getOperation(args, origArgs) //Shifting left to match opName location on LINK interface
	viewArgs := strings.Split(origArgs[endPos:], " ")          //Note: There's probably a more efficient solution for this
	//Need to skip "AS" and "UPDATE"
	vKey, vBucket, vCrdtType := viewArgs[3], viewArgs[4], viewArgs[5]
	keyParamsView := db.getKeyParams(vKey, vBucket, vCrdtType)
	db.AddObject(keyParamsView)
	fmt.Printf("[TRIGGER]View Key %s, Bucket %s, CRDTType %s\n", vKey, vBucket, vCrdtType)
	fmt.Println("[TRIGGER]Processing second half arguments")
	viewOpName, viewArgs, _ := getOperation(viewArgs[3:], origArgs[endPos:]) //Shifting left to match opName locaiton on LINK interface

	firstLinkPart := processLinkOp(keyParamsObj, objOpName, objArgs)
	secondLinkPart := processLinkOp(keyParamsView, viewOpName, viewArgs)

	autoUpd := db.AddLink(firstLinkPart, secondLinkPart)
	tpchInterface.SendTrigger(autoUpd, false, ci)
}

func doDelete(args []string) {
	//TODO
}

func doGetTriggers() {
	tpchInterface.GetTriggers()
	//TODO: Actually do something with this
}

func readTriggerArgs() (args []string, origArgs string) {
	var sb strings.Builder
	var line string
	for i := 0; i < 5; i++ {
		//5 lines to read
		line, _ = reader.ReadString('\n')
		sb.WriteString(line[:len(line)-1])
		sb.WriteRune(' ')
	}
	result := sb.String()
	return strings.Split(result, " "), result
}

func processLinkOp(keyParams antidote.KeyParams, opName string, args []string) antidote.Link {
	opType := getOpTypeFromName(opName, keyParams.CrdtType)
	typifiedArgs := typifyArgs(args)
	return antidote.Link{KeyParams: keyParams, OpType: opType, Arguments: typifiedArgs}
}

func typifyArgs(args []string) (typifiedArgs []interface{}) {
	typifiedArgs = make([]interface{}, len(args))
	for i, sArg := range args {
		if sArg[0] >= '0' && sArg[0] <= '9' {
			//Int
			typifiedArgs[i], _ = strconv.Atoi(sArg)
		} else if sArg[0] == '\'' || sArg[0] == '"' {
			//String
			typifiedArgs[i] = sArg[1 : len(sArg)-1]
		} else {
			//Variable
			typifiedArgs[i] = antidote.Variable{Name: sArg}
		}
	}
	return
}

func getOperation(args []string, origArgs string) (operationName string, opArgs []string, endIndex int) {
	fmt.Println("[OP]Searching for:", args[3])
	start := strings.Index(origArgs, args[3])
	start += len(args[3]) + 1
	fmt.Printf("[LINK]Orig string: %s, startPos: %d, charAtStartPos: %c\n", origArgs, start, origArgs[start])
	//Cutting to the start of operation name
	//origArgs = strings.TrimLeft(origArgs[start:], " ")
	origArgs = origArgs[start:]

	fmt.Println("[LINK]OrigArgs: " + origArgs)
	//operation name until the last ')'
	startArgs, endArgs := strings.IndexRune(origArgs, '('), strings.IndexRune(origArgs, ')')
	fmt.Printf("[LINK]Parenthisis positions: %d (char %c), %d (char %c)\n", startArgs,
		origArgs[startArgs], endArgs, origArgs[endArgs])
	operation := origArgs[:endArgs]
	operationName = operation[:startArgs]
	fmt.Printf("[LINK]Operation name: %s\n", operationName)
	opArgs = getArguments(operation[startArgs+1 : endArgs]) //Removing '(' and ')'
	return operationName, opArgs, endArgs + start + 1
}

func getArguments(opArgs string) []string {
	splitArgs := strings.Split(opArgs, ",")
	fmt.Print("[LINK]Operation args: ")
	for i, arg := range splitArgs {
		splitArgs[i] = strings.TrimSpace(arg)
		fmt.Print(splitArgs[i] + "; ")
	}
	fmt.Println()
	return splitArgs
}

func getReadString(crdtType proto.CRDTType, protobuf *proto.ApbReadObjectResp) string {
	return tools.StateToString(crdt.ReadRespProtoToAntidoteState(protobuf, crdtType, proto.READType_FULL))
}

//Note: These args don't include key, bucket or crdtType. args[0] contains the operation name.
func processUpdArgs(crdtType proto.CRDTType, args []string) crdt.UpdateArguments {
	opType := getOpTypeFromName(args[0], crdtType)
	switch crdtType {
	case proto.CRDTType_COUNTER:
		fmt.Printf("[UPDATE][COUNTER]Op name: %s, Value: %s\n", args[0], args[1])
		value, _ := strconv.ParseInt(args[1], 10, 32)
		fmt.Printf("[UPDATE][COUNTER]Processed value: %d\n", value)
		if opType == antidote.DEC {
			return crdt.Decrement{Change: int32(value)}
		}
		return crdt.Increment{Change: int32(value)}

	case proto.CRDTType_LWWREG:
		fmt.Printf("[UPDATE][LWWREG]Op name: %s, Value: %v\n", args[0], args[1])
		return crdt.SetValue{NewValue: interface{}(args[1])}

	case proto.CRDTType_RWSET:
		fmt.Printf("[UPDATE][RWSET]Op name: %s, Value: %v\n", args[0], args[1])
		if opType == antidote.SET_ADD {
			return crdt.Add{Element: crdt.Element(args[1])}
		}
		return crdt.Remove{Element: crdt.Element(args[1])}

	case proto.CRDTType_TOPK_RMV:
		fmt.Printf("[UPDATE][TOPK]Op name: %s, Value: %s\n", args[0], args[1])
		id, _ := strconv.ParseInt(args[1], 10, 32)
		if opType == antidote.TOPK_ADD {
			//id, Score. We're ignoring byte array of data.
			score, _ := strconv.ParseInt(args[2], 10, 32)
			fmt.Printf("[UPDATE][TOPKADD]Id %d, Score %d\n", id, score)
			return crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: int32(id), Score: int32(score), Data: nil}}
		}
		//remove. ID only
		fmt.Printf("[UPDATE][TOPKREMOVE]Id: %d\n", id)
		return crdt.TopKRemove{Id: int32(id)}

	case proto.CRDTType_AVG:
		fmt.Printf("[UPDATE][AVG]Op name: %s, Value: %s\n", args[0], args[1])
		value, _ := strconv.ParseInt(args[1], 10, 32)
		if opType == antidote.AVG_MULTIPLE_ADD {
			nAdds, _ := strconv.ParseInt(args[2], 10, 32)
			fmt.Printf("[UPDATE][AVG_ADD_M]Value: %d, NAdds: %d\n", value, nAdds)
			return crdt.AddMultipleValue{SumValue: value, NAdds: nAdds}
		}
	case proto.CRDTType_MAXMIN:
		fmt.Printf("[UPDATE][MAXMIN]Op name: %s, Value: %s\n", args[0], args[1])
		value, _ := strconv.ParseInt(args[1], 10, 32)
		if opType == antidote.MAX_ADD {
			fmt.Printf("[UPDATE][MAXADD]Value: %d\n", value)
			return crdt.MaxAddValue{Value: int64(value)}
		}
		fmt.Printf("[UPDATE][MINADD]Value: %d\n", value)
		return crdt.MinAddValue{Value: int64(value)}

	case proto.CRDTType_TOPSUM:
		fmt.Printf("[UPDATE][TOPSUM]Op name: %s, Id: %v, Score: %v\n", args[0], args[1], args[2])
		id, _ := strconv.ParseInt(args[1], 10, 32)
		score, _ := strconv.ParseInt(args[2], 10, 32)
		return crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: int32(id), Score: int32(score), Data: nil}}
	}

	return nil
}

func getOpTypeFromName(name string, crdtType proto.CRDTType) antidote.OpType {
	name = strings.ToUpper(name)
	switch crdtType {
	case proto.CRDTType_COUNTER:
		if strings.HasPrefix(name, "DEC") {
			fmt.Println("Operation identified as DEC")
			return antidote.DEC
		}
		fmt.Println("Operation identified as INC")
		return antidote.INC
	case proto.CRDTType_LWWREG:
		return antidote.LWW_ADD
	case proto.CRDTType_RWSET:
		if strings.HasSuffix(name, "ADD") {
			return antidote.SET_ADD
		}
		return antidote.SET_RMV

	case proto.CRDTType_TOPK_RMV:
		if strings.HasSuffix(name, "ADD") {
			return antidote.TOPK_ADD
		}
		return antidote.TOPK_RMV
	case proto.CRDTType_AVG:
		if strings.Contains(name, "MULTIPLE") {
			fmt.Println("Identified as AVG_MULTIPLE_ADD:", name)
			return antidote.AVG_MULTIPLE_ADD
		}
		fmt.Println("Identified as AVG_ADD:", name)
		return antidote.AVG_ADD
	case proto.CRDTType_MAXMIN:
		if strings.Contains(name, "MAX") {
			return antidote.MAX_ADD
		}
		return antidote.MIN_ADD
	case proto.CRDTType_TOPSUM:
		return antidote.TOPSUM_ADD
	}
	fmt.Println("Warning - unsupported crdt or operation type:", crdtType, name)
	return -1
}

func ignore(any ...interface{}) {

}
