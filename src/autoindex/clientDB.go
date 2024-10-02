package autoindex

//TODO: If we decide to keep this, we should add a GetOpType() to every op.
//TODO: Maybe generic triggers could be stored by CRDTType (and maybe opType)?
//That way the matching is quicker, as a lot less triggers need to be tested.
//Also can't really store by bucket as that might be generic too.

import (
	"fmt"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"strings"
)

type ClientDB struct{ antidote.TriggerDB }

func InitializeClientDB() ClientDB {
	return ClientDB{TriggerDB: antidote.InitializeTriggerDB()}
}

func (db ClientDB) getKeyParams(key, bucket, crdtType string) crdt.KeyParams {
	return crdt.KeyParams{Key: key, Bucket: bucket, CrdtType: db.stringTypeToCrdtType(crdtType)}
}

func (db ClientDB) stringTypeToCrdtType(crdtType string) proto.CRDTType {
	return proto.CRDTType(proto.CRDTType_value[strings.ToUpper(crdtType)])
}

// For each link, prepares the respective update
func (db ClientDB) prepareLinks(params crdt.UpdateObjectParams) []crdt.UpdateObjectParams {
	keyParams := params.KeyParams
	genericUpds := make([]crdt.UpdateObjectParams, antidote.DEFAULT_GENERIC_SIZE)
	i := 0
	//Checks generic first
	for matchKeyP, links := range db.GenericMapping {
		if matchKeyP.Match(keyParams) {
			fmt.Printf("[PREPARE_LINKS]Generic key match! %v ||| %v\n", matchKeyP, keyParams)
			for _, link := range links {
				if db.opTypeMatches(link.Trigger.OpType, params.UpdateArgs) {
					fmt.Println("[PREPARE_LINKS]Generic link match!")
					genericUpds[i] = db.generateUpdate(link, params)
					i++
				}
			}
		}
	}
	genericUpds = genericUpds[:i]

	links := db.Mapping[keyParams]
	upds := make([]crdt.UpdateObjectParams, len(links)+1+i) //space for params as well

	for _, link := range links {
		fmt.Println("[PREPARE_LINKS]Processing link: ", link)
		if db.opTypeMatches(link.Trigger.OpType, params.UpdateArgs) {
			fmt.Println("[PREPARE_LINKS]Match.")
			fmt.Printf("[PREPARE_LINKS]Trigger opType %d, Op %v, Target opType %d.\n", link.Trigger.OpType,
				params.UpdateArgs, link.Target.OpType)
			upds[i] = db.generateUpdate(link, params)
			i++
		}
	}
	upds[i] = params
	copy(upds, genericUpds) //Will replace first positions which are empty
	i++
	return upds[:i]
}

func (db ClientDB) opTypeMatches(triggerType antidote.OpType, updArgs crdt.UpdateArguments) (matches bool) {
	switch (updArgs).(type) {
	case crdt.Increment:
		return triggerType == antidote.INC
	case crdt.Decrement:
		return triggerType == antidote.DEC
	case crdt.Add:
		return triggerType == antidote.SET_ADD
	case crdt.Remove:
		return triggerType == antidote.SET_RMV
	case crdt.SetValue:
		return triggerType == antidote.LWW_ADD

	case crdt.TopKAdd:
		return triggerType == antidote.TOPK_ADD
	case crdt.TopKRemove:
		return triggerType == antidote.TOPK_RMV
	case crdt.AddValue:
		return triggerType == antidote.AVG_ADD
	case crdt.AddMultipleValue:
		return triggerType == antidote.AVG_MULTIPLE_ADD
	case crdt.MaxAddValue:
		return triggerType == antidote.MAX_ADD
	case crdt.MinAddValue:
		return triggerType == antidote.MIN_ADD
	case crdt.TopSAdd:
		return triggerType == antidote.TOPSUM_ADD
	case crdt.TopSSub:
		return triggerType == antidote.TOPSUM_SUB
	}
	return false
}

func (db ClientDB) generateUpdate(updInfo antidote.AutoUpdate, params crdt.UpdateObjectParams) crdt.UpdateObjectParams {
	updArgs := params.UpdateArgs
	varsNames := updInfo.Trigger.Arguments

	varsValues := make(map[string]interface{}, len(updInfo.Target.Arguments))
	switch updArgs.GetCRDTType() {
	case proto.CRDTType_COUNTER:
		switch counter := updArgs.(type) {
		case crdt.Increment:
			varsValues[varsNames[0].(antidote.Variable).Name] = counter.Change
		case crdt.Decrement:
			varsValues[varsNames[0].(antidote.Variable).Name] = counter.Change
		}

	case proto.CRDTType_LWWREG:
		varsValues[varsNames[0].(antidote.Variable).Name] = (updArgs.(crdt.SetValue).NewValue)

	case proto.CRDTType_ORSET:
		switch set := updArgs.(type) {
		case crdt.Add:
			varsValues[varsNames[0].(antidote.Variable).Name] = string(set.Element)
		case crdt.Remove:
			varsValues[varsNames[0].(antidote.Variable).Name] = string(set.Element)
		}

	case proto.CRDTType_TOPK_RMV:
		switch topK := updArgs.(type) {
		case crdt.TopKAdd:
			varsValues[varsNames[0].(antidote.Variable).Name], varsValues[varsNames[1].(antidote.Variable).Name] = topK.Id, topK.Score
		case crdt.TopKRemove:
			varsValues[varsNames[0].(antidote.Variable).Name] = topK.Id
		}

	case proto.CRDTType_AVG:
		switch avg := updArgs.(type) {
		case crdt.AddValue:
			varsValues[varsNames[0].(antidote.Variable).Name] = avg.Value
		case crdt.AddMultipleValue:
			varsValues[varsNames[0].(antidote.Variable).Name], varsValues[varsNames[1].(antidote.Variable).Name] = avg.SumValue, avg.NAdds
		}

	case proto.CRDTType_MAXMIN:
		switch maxmin := updArgs.(type) {
		case crdt.MaxAddValue:
			varsValues[varsNames[0].(antidote.Variable).Name] = maxmin.Value
		case crdt.MinAddValue:
			varsValues[varsNames[0].(antidote.Variable).Name] = maxmin.Value
		}

	case proto.CRDTType_TOPSUM:
		switch topsum := updArgs.(type) {
		case crdt.TopSAdd:
			varsValues[varsNames[0].(antidote.Variable).Name], varsValues[varsNames[1].(antidote.Variable).Name] = topsum.Id, topsum.Score
		case crdt.TopSSub:
			varsValues[varsNames[0].(antidote.Variable).Name], varsValues[varsNames[1].(antidote.Variable).Name] = topsum.Id, topsum.Score
		}
	}

	fmt.Println("Target op type:", updInfo.Target.OpType)
	varsToReplace := updInfo.Target.Arguments
	var generatedUpd crdt.UpdateArguments
	switch updInfo.Target.OpType {
	case antidote.INC:
		generatedUpd = crdt.Increment{Change: db.getInt32ForArg(varsToReplace[0], varsValues)}
	case antidote.DEC:
		generatedUpd = crdt.Decrement{Change: db.getInt32ForArg(varsToReplace[0], varsValues)}
	case antidote.LWW_ADD:
		generatedUpd = crdt.SetValue{NewValue: db.getInterfaceForArg(varsToReplace[0], varsValues)}
	case antidote.SET_ADD:
		generatedUpd = crdt.Add{Element: db.getElementForArg(varsToReplace[0], varsValues)}
	case antidote.SET_RMV:
		generatedUpd = crdt.Remove{Element: db.getElementForArg(varsToReplace[0], varsValues)}

	case antidote.TOPK_ADD:
		generatedUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{
			Id: db.getInt32ForArg(varsToReplace[0], varsValues), Score: db.getInt32ForArg(varsToReplace[1], varsValues)}}
	case antidote.TOPK_RMV:
		generatedUpd = crdt.TopKRemove{Id: db.getInt32ForArg(varsToReplace[0], varsValues)}
	case antidote.AVG_ADD:
		generatedUpd = crdt.AddValue{Value: int64(db.getInt32ForArg(varsToReplace[0], varsValues))}
	case antidote.AVG_MULTIPLE_ADD:
		generatedUpd = crdt.AddMultipleValue{SumValue: int64(db.getInt32ForArg(varsToReplace[0], varsValues)),
			NAdds: int64(db.getInt32ForArg(varsToReplace[1], varsValues))}
	case antidote.MAX_ADD:
		generatedUpd = crdt.MaxAddValue{Value: int64(db.getInt32ForArg(varsToReplace[0], varsValues))}
	case antidote.MIN_ADD:
		generatedUpd = crdt.MinAddValue{Value: int64(db.getInt32ForArg(varsToReplace[0], varsValues))}
	case antidote.TOPSUM_ADD:
		generatedUpd = crdt.TopSAdd{TopKScore: crdt.TopKScore{
			Id: db.getInt32ForArg(varsToReplace[0], varsValues), Score: db.getInt32ForArg(varsToReplace[1], varsValues)}}
	case antidote.TOPSUM_SUB:
		generatedUpd = crdt.TopSSub{TopKScore: crdt.TopKScore{
			Id: db.getInt32ForArg(varsToReplace[0], varsValues), Score: db.getInt32ForArg(varsToReplace[1], varsValues)}}
	}

	fmt.Println("Generated op:", generatedUpd)
	return crdt.UpdateObjectParams{KeyParams: updInfo.Target.KeyParams, UpdateArgs: generatedUpd}
}

func (db ClientDB) getInt32ForArg(toReplace interface{}, varsValues map[string]interface{}) int32 {
	fmt.Printf("[getInt32ForArg]Type: %T\n", toReplace)
	switch typed := toReplace.(type) {
	case int:
		return int32(typed)
	case antidote.Variable:
		return varsValues[typed.Name].(int32)
	}
	fmt.Printf("[getInt32ForArg]Unexpected type. Replacing %v failed given %v\n", toReplace, varsValues)
	return -1
}

func (db ClientDB) getStringForArg(toReplace interface{}, varsValues map[string]interface{}) string {
	switch typed := toReplace.(type) {
	case string:
		return typed
	case antidote.Variable:
		return varsValues[typed.Name].(string)
	}
	fmt.Printf("[getStringForArg]Unexpected type. Replacing %v failed given %v\n", toReplace, varsValues)
	return ""
}

func (db ClientDB) getElementForArg(toReplace interface{}, varsValues map[string]interface{}) crdt.Element {
	switch typed := toReplace.(type) {
	case crdt.Element:
		return typed
	case antidote.Variable:
		return varsValues[typed.Name].(crdt.Element)
	}
	fmt.Printf("[getElementForArg]Unexpected type. Replacing %v failed given %v\n", toReplace, varsValues)
	return ""
}

func (db ClientDB) getInterfaceForArg(toReplace interface{}, varsValues map[string]interface{}) interface{} {
	switch typed := toReplace.(type) {
	case antidote.Variable:
		return varsValues[typed.Name].(interface{})
	default:
		return typed
	}
}
